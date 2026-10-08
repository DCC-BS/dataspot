import os
from dotenv import load_dotenv
import logging
import json
import pandas as pd
from pathlib import Path
import pyodbc # add this to requirements.txt?

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
# - upload to dataspot
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
 
 
def build_structure(df: pd.DataFrame) -> list[dict]:
    top_path = f"{ROOT_PATH}/{TOP_COLLECTION}"
 
    collections = {}   # path -> entry (dict keeps insertion order, no duplicates)
    datasets = []
 
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
 
        datasets.append({
            "_type": "Dataset",
            "label": label,
            "inCollection": parent_path,
        })
 
    datasets.sort(key=lambda d: (d["inCollection"], d["label"]))
    return list(collections.values()) + datasets


def main():
    """
    Main control for the ETL process.
    """
    logger.info("--- ETL Process Start: Stata Products -> Data Catalog ---")

    df = load_data()
    result = build_structure(df)
    with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
        json.dump(result, f, ensure_ascii=False, indent=2)
    n_coll = sum(e["_type"] == "Collection" for e in result)
    logger.info(f"Wrote {n_coll} collections and {len(result) - n_coll} datasets to {OUTPUT_FILE}")


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(filename)s:%(lineno)d %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
        force=True,
        )
    main()