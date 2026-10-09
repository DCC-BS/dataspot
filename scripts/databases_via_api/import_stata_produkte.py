import os
from dotenv import load_dotenv
import logging
import json
import pandas as pd
from pathlib import Path
import pyodbc # add this to requirements.txt?
import config
from src.clients.base_client import BaseDataspotClient
from src.clients.helpers import url_join
from src.common import requests
import re

# --- CONFIGURATION ---

load_dotenv()

server = os.getenv("STATA_PRODUKTE_SERVERNAME")
username = os.getenv("STATA_PRODUKTE_USERNAME")
password = os.getenv("STATA_PRODUKTE_PASSWORD")

ROOT_PATH = "Regierung und Verwaltung/Präsidialdepartement/Statistisches Amt"
TOP_COLLECTION = "Öffentliche Statistik"
OUTPUT_FILE = Path(__file__).parent / "Oeffentliche_Statistik_export.json"

# TODO: uncomment these once we implement Load step
#STATUS_ON_CREATE = 'PUBLISHED'
#STATUS_ON_UPDATE = 'PUBLISHED'
#STATUS_ON_DELETE = None # None: Delete directly


# Logging configuration
# TODO: Not sure how we want to implement logging so that it works system-wide
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


# TODO: 
# - add description
# - add keywords
# - clean code

def load_data() -> pd.DataFrame:

    original_https_proxy = os.environ.get("HTTPS_PROXY")
    os.environ["HTTPS_PROXY"] = ""

    try:
        conn_str = (
            "DRIVER={ODBC Driver 17 for SQL Server};"
            f"SERVER={server};"
            f"UID={username};"
            f"PWD={password};"
            "Trusted_Connection=yes;"
        )

        with pyodbc.connect(conn_str) as conn:
            # List all tables in the database
            return pd.read_sql("""
                SELECT TOP (1000)
                [produkt]
                ,[bezeichnung]
                ,[thema]
                ,[Thema_Nr]
                ,[unterthema]
                ,[suchprodukt]
                ,[beschreibung]
                ,[stichwortliste]
            FROM [stata_produkte].[dbo].[vw_stata_website] WHERE produkt = 'Webtabelle'
            """, conn)
        
    finally:
        if original_https_proxy is None:
            os.environ.pop("HTTPS_PROXY", None)
        else:
            os.environ["HTTPS_PROXY"] = original_https_proxy


def clean(value):
    """Return a stripped string, or None for empty/NULL values."""
    if value is None or pd.isna(value):
        return None
    value = str(value).strip()
    return value or None


def parse_tags(value) -> list[str]:
    """Turn 'Haushalte; Wohnformen, Familienhaushalte' into a sorted list of unique tags."""
    value = clean(value)
    if value is None:
        return []
    parts = re.split(r"[;,|\n]", value)
    tags = {p.strip() for p in parts if p.strip()}
    return sorted(tags, key=str.lower)
 
 
def build_structure(df: pd.DataFrame) -> list[dict]:
    top_path = f"{ROOT_PATH}/{TOP_COLLECTION}"
 
    collections = {}   # path -> entry (dict keeps insertion order, no duplicates)
    datasets = []
    seen_datasets = set()
 
    collections[top_path] = {
        "_type": "Collection",
        "label": TOP_COLLECTION,
        "inCollection": ROOT_PATH,
    }

    # do we actually need to sort this?
    df = df.sort_values(["Thema_Nr", "unterthema", "bezeichnung"], key=lambda s: s.astype(str).str.zfill(2), na_position="last")
 
    for _, row in df.iterrows():
        label = clean(row["bezeichnung"])
        thema = clean(row["thema"])
        unterthema = clean(row["unterthema"])
        beschreibung = clean(row["beschreibung"])
        stichwortliste = parse_tags(row["stichwortliste"])

        if label is None or thema is None:
            print(f"Skipped (missing bezeichnung or thema): {row.to_dict()}")
            continue
 
        thema_path = f"{top_path}/{thema}"
        collections.setdefault(thema_path, {
            "_type": "Collection",
            "label": thema,
            "inCollection": top_path,
            "description": beschreibung,
            "tags": stichwortliste
        })
 
        parent_path = thema_path
        if unterthema is not None:
            parent_path = f"{thema_path}/{unterthema}"
            collections.setdefault(parent_path, {
                "_type": "Collection",
                "label": unterthema,
                "inCollection": thema_path,
                "description": beschreibung,
                "tags": stichwortliste
            })
 
        dataset_path = f"{parent_path}/{label}"
        if dataset_path in seen_datasets:
            logger.warning(f"Duplicate dataset skipped: {dataset_path}")
            continue
        seen_datasets.add(dataset_path)

        datasets.append({
            "_type": "Dataset",
            "label": label + " (ÖS)", # to make sure that a dataset does not have the same name as its collection and also to ensure uniqueness in dataspot
            "inCollection": parent_path,
            "description": beschreibung,
            "tags": stichwortliste
        })
 
    datasets.sort(key=lambda d: (d["inCollection"], d["label"]))
    return list(collections.values()) + datasets


def upload_to_dataspot(assets: list[dict]):

    logger.info("Uploading now")
 
    client = BaseDataspotClient(scheme_name=config.dnk_scheme_name, scheme_name_short=config.dnk_scheme_name_short)
 
    client.bulk_create_or_update_assets(
        scheme_name=client.scheme_name,
        data=assets,
        operation="REPLACE",
        on_delete = "DELETENEW"
    )

def main():
    """
    Main control for the ETL process.
    """
    logger.info("--- ETL Process Start: Stata Products -> Data Catalog ---")

    df = load_data()
    result = build_structure(df)
    """ with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
        json.dump(result, f, ensure_ascii=False, indent=2) """
    n_coll = sum(e["_type"] == "Collection" for e in result)
    logger.info(f"Wrote {n_coll} collections and {len(result) - n_coll} datasets to {OUTPUT_FILE}")

    upload_to_dataspot(result)



if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(filename)s:%(lineno)d %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
        force=True,
        )
    main()