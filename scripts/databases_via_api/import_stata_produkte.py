import os
from typing import Any, Dict, List, Tuple
from dotenv import load_dotenv
import logging
import json
import pandas as pd
from pathlib import Path
from mssql_python import connect # add to requirements.txt?
import config
from src.clients.base_client import BaseDataspotClient
from src.clients.helpers import url_join
from src.common import requests
from src.clients.helpers import parse_iso_date_utc_midnight_ms
import re
from urllib.parse import quote
from src.clients.dnk_client import DNKClient

# --- CONFIGURATION ---

load_dotenv()

server = os.getenv("STATA_PRODUKTE_SERVERNAME")
username = os.getenv("STATA_PRODUKTE_USERNAME")
password = os.getenv("STATA_PRODUKTE_PASSWORD")

# TOOD: Future-proof me by only defining the UUID
ROOT_PATH = "Regierung und Verwaltung/Präsidialdepartement/Statistisches Amt"
TOP_COLLECTION = "Öffentliche Statistik"
OUTPUT_FILE = Path(__file__).parent / "Oeffentliche_Statistik_export.json"

# Logging configuration
# TODO: Not sure how we want to implement logging so that it works system-wide
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

QUERY = """ SELECT [id],[produkt],[bezeichnung],[thema],[Thema_Nr],[unterthema],[suchprodukt],
            [beschreibung],[stichwortliste],[naechste_aktualisierung],[zeitreihe_start_jahr],
            [zeitreihe_ende_jahr],[email]
            FROM [stata_produkte].[dbo].[vw_stata_website] WHERE produkt = 'Webtabelle' """

PEOPLE_QUERY = """select u.login_id, p.given_name, p.family_name from user_view u join person_view p on p.id = u.is_person"""

#TODO: add statistikportal as system

def load_data() -> pd.DataFrame:

    original_https_proxy = os.environ.get("HTTPS_PROXY")
    os.environ["HTTPS_PROXY"] = ""

    try:
        conn_str = (
            f"Server={server};"
            f"UID={username};"
            f"PWD={password};"
            "Encrypt=yes;"
            "TrustServerCertificate=yes;"
        )
 
        conn = connect(conn_str)
        try:
            cursor = conn.cursor()
            cursor.execute(QUERY, ("Webtabelle",))
            columns = [col[0] for col in cursor.description]
            rows = [tuple(row) for row in cursor.fetchall()]
            cursor.close()
            return pd.DataFrame.from_records(rows, columns=columns)
        finally:
            conn.close()
        
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
    value = clean(value)
    if value is None:
        return []
    parts = re.split(r"[;,|\n]", value)
    # Strip whitespace and leading list markers like "- " or "– "
    tags = {re.sub(r"^[\s\-–•]+", "", p).strip() for p in parts}
    tags.discard("")
    return sorted(tags, key=str.lower)


# to escape slash in label, e.g. for dataset "Abend-/Nachtarbeit von Erwerbstätigen (StatA)" in "03 Arbeit und Erwerb"
def escape(value: str) -> str:
    # quote a segment if it contains / or "
    if "/" in value or '"' in value:
        return '"' + value.replace('"', '""') + '"'
    return value


def build_name_lookup(rows: List[Dict[str, Any]]) -> Dict[str, Tuple[str, str]]:
    """email -> (family_name, given_name), case-insensitive"""
    return {
        str(r["login_id"]).strip().lower(): (r["family_name"], r["given_name"])
        for r in rows
        if r.get("login_id")
    }


def q(value: str) -> str:
    return '"' + value.replace('"', '""') + '"'

# TODO: add people for: Mathias.Bestgen@bs.ch, joelle.velvart@bs.ch, Michael.Muelli@bs.ch, Martina.Schriber@bs.ch, Matthias.Minke@bs.ch
def person_key_from_email(email: str, names: Dict[str, Tuple[str, str]]) -> str:
    key = email.strip().lower()
    if key not in names:
        #TODO: how to handle this?
        raise KeyError(f"No dataspot person with email {email!r}")
    family, given = names[key]
    given_part = q(given) if (" " in given or '"' in given) else given
    return q(f'{q(family)} {given_part}')


def build_structure(df: pd.DataFrame, people: Dict[str, Tuple[str, str]]) -> tuple[list[dict], list[dict]]:
    top_path = f"{ROOT_PATH}/{TOP_COLLECTION}"
 
    collections = {}   # path -> entry (dict keeps insertion order, no duplicates)
    datasets = []
    attributions = []
    distributions = []
    seen_datasets = set()
 
    collections[top_path] = {
        "_type": "Collection",
        "label": TOP_COLLECTION,
        "inCollection": ROOT_PATH,
    }

    df = df.sort_values(["Thema_Nr", "unterthema", "bezeichnung"], key=lambda s: s.astype(str).str.zfill(2), na_position="last")
 
    for _, row in df.iterrows():
        label = clean(row["bezeichnung"])
        thema = clean(row["thema"])
        unterthema = clean(row["unterthema"])
        beschreibung = clean(row["beschreibung"])
        stichwortliste = parse_tags(row["stichwortliste"])
        suchprodukt = parse_tags(row["suchprodukt"])

        email = parse_tags(row["email"])[0]

        tag = parse_tags(row["zeitreihe_ende_jahr"])
        zeitreihe_ende_jahr = int(float(tag[0])) if tag else None # row value would be something like ['2025.0'], we want 2025

        tag = parse_tags(row["zeitreihe_start_jahr"])
        zeitreihe_start_jahr = int(float(tag[0])) if tag else None

        stata_id = parse_tags(row["id"])[0] # anscheinend ist das eine liste, warum auch immer

        tag = parse_tags(row["naechste_aktualisierung"])
        aktualisierung = parse_iso_date_utc_midnight_ms(tag[0]) if tag else None
    
        if label is None or thema is None:
            print(f"Skipped (missing bezeichnung or thema): {row.to_dict()}")
            continue
 
        thema_path = f"{top_path}/{thema}"
        collections.setdefault(thema_path, {
            "_type": "Collection",
            "label": thema,
            "inCollection": top_path,
        })
 
        parent_path = thema_path
        if unterthema is not None:
            parent_path = f"{thema_path}/{unterthema}"
            collections.setdefault(parent_path, {
                "_type": "Collection",
                "label": unterthema,
                "inCollection": thema_path,
            })
 
        dataset_path = f"{parent_path}/{label}"
        if dataset_path in seen_datasets:
            logger.warning(f"Duplicate dataset skipped: {dataset_path}")
            continue
        seen_datasets.add(dataset_path)

        datasets.append({
            "_type": "Dataset",
            "label": label + " (StatA)", # to make sure that a dataset does not have the same name as its collection and also to ensure uniqueness in dataspot
            "inCollection": parent_path,
            "description": beschreibung,
            "tags": stichwortliste,
            "stereotype" : "STATA",
            "stata_search_product": suchprodukt,
            "stata_next_update": aktualisierung,
            "stata_portal_url": "https://statistik.bs.ch/statistik/" + stata_id,
            "stata_theme": thema,
            "stata_zeitreihe_ende_jahr": zeitreihe_ende_jahr,
            "stata_zeitreihe_start_jahr": zeitreihe_start_jahr
        })

        distributions.append({
            "_type": "Distribution",
            "label" : "Statistikportal",
            "distributionOf" : f"{parent_path}/{escape(label + " (StatA)")}",
            "format" : "html",
            "accessURL" : "https://statistik.bs.ch/statistik/" + stata_id,
        })

        attributed_to = person_key_from_email(email, people)

        attributions.append({
            "_type": "Attribution",
            "attributionFor": f"{parent_path}/{escape(label + " (StatA)")}",
            "attributedTo": attributed_to,
            "attributedAs": "Data Steward"
        })
 
    datasets.sort(key=lambda d: (d["inCollection"], d["label"]))
    return (list(collections.values()) + datasets + distributions, attributions)


def upload_to_dataspot(assets: list[dict], status: str, client):

    logger.info("Uploading now")

    client.bulk_create_or_update_assets(
        scheme_name=client.scheme_name,
        data=assets,
        operation="REPLACE",
        status = status
    )


def main():
    """
    Main control for the ETL process.
    """
    logger.info("--- ETL Process Start: Stata Products -> Data Catalog ---")

    client = DNKClient()

    df = load_data()
    people = build_name_lookup(client.execute_query_api(PEOPLE_QUERY))

    result, attributions = build_structure(df, people)
    n_coll = sum(e["_type"] == "Collection" for e in result)
    logger.info(f"Wrote {n_coll} collections and {len(result) - n_coll} datasets to {OUTPUT_FILE}")

    """  with open("attributions.json", "w", encoding="utf-8") as file:
        json.dump(attributions, file, indent=2, ensure_ascii=False) """

    upload_to_dataspot(result, "PUBLISHED", client)
    upload_to_dataspot(attributions, None, client)



if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(filename)s:%(lineno)d %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
        force=True,
        )
    main()