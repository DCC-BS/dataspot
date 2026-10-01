import argparse
import json
import logging
import os
from typing import Any, Dict, List, Tuple
from dotenv import load_dotenv
import config
from src.clients.base_client import BaseDataspotClient
from src.clients.helpers import url_join
from src.common import requests
from requests_ntlm import HttpNtlmAuth

load_dotenv()

AUE_AD_USERNAME = os.getenv("AUE_AD_USERNAME")
AUE_AD_PASSWORD = os.getenv("AUE_AD_PASSWORD")

REALM = "BS.CH"

username = f"{REALM}\\{AUE_AD_USERNAME}"
password = AUE_AD_PASSWORD

BRYTECUBE_VIEWS_API_URL = "https://brytecube.aue.wsu.bs.ch/BcOdata/WebApi/ResourceMetadata/AllViews/46ba9cdc-f416-421b-a38f-75715f6a1554"

# for "inCollection" value
BRYTECUBE_COLLECTION_PATH = ("Regierung und Verwaltung/Departement für Wirtschaft, Soziales und Umwelt/Amt für Umwelt und Energie (AUE)/BryteCube")

# Identifies this script as the owner of the Datenprodukte it uploads. With operation=REPLACE,
# only assets carrying this agentId are considered obsolete, so Datenprodukte created by hand
# (or by other scripts) in the BryteCube collection are never touched.
BRYTECUBE_AGENT_ID = "brytecube-sync"
 
# Status set on Datenprodukte that are no longer returned by the BryteCube API
STALE_STATUS = "DELETENEW"

##### Questions: 
# - certificate?
# - more information on views via ty-id?


def _extract_upload_errors(response_json: Any) -> List[str]:
    """Pull out ERROR-level messages from a bulk upload API response."""
    errors: List[str] = []
    if not isinstance(response_json, list):
        return errors
    for item in response_json:
        if not isinstance(item, dict):
            continue
        if (item.get("level") or "").upper() != "ERROR":
            continue
        message = (item.get("message") or "").strip()
        if message:
            errors.append(message)
    return errors


def fetch_brytecube_views(url: str = BRYTECUBE_VIEWS_API_URL) -> List[Dict[str, Any]]:
    
    logging.info(f"Fetching BryteCube views from {url} ...")

    # Disable proxy to connect to BryteCube in the internal network, and always restore it
    # afterwards (even on error) so the Dataspot calls go through the proxy again.
    original_https_proxy = os.environ.get("HTTPS_PROXY")
    os.environ["HTTPS_PROXY"] = ""
    try:
        # NOTE: verify=False is only a quick and dirty fix, in the long run a certificate is needed (from AUE or IT?)
        response = requests.get(url, auth=HttpNtlmAuth(username=username, password=password), verify=False)
        response.raise_for_status()
    finally:
        if original_https_proxy is None:
            os.environ.pop("HTTPS_PROXY", None)
        else:
            os.environ["HTTPS_PROXY"] = original_https_proxy
 
    views = response.json()
    if not isinstance(views, list):
        raise ValueError(f"Unexpected response format from BryteCube API. Expected a list but got: {type(views)}")

    logging.info(f"Fetched {len(views)} view(s) from the BryteCube API")
    return views


def _extract_view_names_and_descriptions(views: List[Dict[str, Any]]) -> List[Tuple[str, str]]:
    """
    Extract the TY_LONGNAME and TY_DESCRIPTION fields from each BryteCube view record.
    Entries missing TY_LONGNAME are skipped and logged.
    """
    results: List[Tuple[str, str]] = []
    seen = set()
    for view in views:
        name = (view.get("TY_LONGNAME") or "").strip()
        if not name:
            logging.warning(f"Skipping view with missing/blank TY_LONGNAME: {view}")
            continue
        if name in seen:
            logging.warning(f"Duplicate TY_LONGNAME '{name}' - keeping only the first occurrence")
            continue
        seen.add(name)
 
        description = (view.get("TY_DESCRIPTION") or "").strip()
        if not description:
            logging.debug(f"View '{name}' has no TY_DESCRIPTION")
        results.append((name, description))
    return results


def _build_collection_asset() -> Dict[str, Any]:
    """
    Build the asset dict for the BryteCube collection itself.
 
    It has to be part of the upload: with operation=REPLACE, dataspot only reconciles the
    children of parents that are included in the upload.
    """
    parent_path, _, collection_label = BRYTECUBE_COLLECTION_PATH.rpartition("/")
    return {
        "_type": "Collection",
        "label": collection_label,
        "inCollection": parent_path,
    }


def _build_dataset_assets(views: List[Tuple[str, str]]) -> List[Dict[str, Any]]:
    """
    Build bulk-upload asset dicts for the given (name, description) pairs
    """
    assets = []
    for name, description in views:
        asset = {
            "_type": "Dataset",
            "label": name + " (AUE)",
            "inCollection": BRYTECUBE_COLLECTION_PATH,
        }
        if description:
            asset["description"] = description
        assets.append(asset)
    return assets


def _upload_assets(assets: List[Dict[str, Any]], dry_run: bool = False) -> Any:
    """
    Reconcile the BryteCube collection in the Datenprodukte (DNK) scheme via the bulk upload API.
 
    - New views are added, existing ones are updated (matched by label).
    - Datenprodukte previously uploaded by this script that are no longer in the upload are set to STALE_STATUS.
    - Existing statuses are left unchanged (status=None).
    """
    # NOTE: remove this once the script goes live
    if config.database_name != "test-aue-brytecube-api":
        logging.warning(
            f"config.database_name is '{config.database_name}', not the expected "
            "'test-aue-brytecube-api'. Double-check config.py before proceeding."
        )
 
    client = BaseDataspotClient(scheme_name=config.dnk_scheme_name, scheme_name_short=config.dnk_scheme_name_short)
 
    logging.info(
        f"Uploading {len(assets)} asset(s) to scheme '{client.scheme_name}' in database "
        f"'{config.database_name}' (operation=REPLACE, onDelete={STALE_STATUS}, "
        f"agentId={BRYTECUBE_AGENT_ID}, dryRun={dry_run})"
    )
 
    response = client.bulk_create_or_update_assets(
        scheme_name=client.scheme_name,
        data=assets,
        operation="REPLACE",
        on_delete=STALE_STATUS,
        #on_insert="PUBLISHED", #default is "WORKING", set to "PUBLISHED" on prod
        agent_id=BRYTECUBE_AGENT_ID,
        status=None,  # don't reset the status of the collection to WORKING
        dry_run=dry_run,
    )
 
    errors = _extract_upload_errors(response)
    if errors:
        logging.error(f"Upload completed with {len(errors)} error(s):")
        for error in errors:
            logging.error(f"  - {error}")
    else:
        logging.info("Upload completed without errors.")
 
    return response


def upload_brytecube_datenprodukte_from_api(url: str = BRYTECUBE_VIEWS_API_URL, dry_run: bool = False) -> Any:
    views = fetch_brytecube_views(url)
    names = _extract_view_names_and_descriptions(views)

    logging.info(f"Extracted {len(names)} unique Datenprodukt name(s) from TY_LONGNAME")
 
    if not names:
        # Safety net: with operation=REPLACE, uploading the collection without any Datasets
        # would mark every Datenprodukt in it as obsolete. Never do that just because the
        # BryteCube API returned nothing (transient error, auth failure that still returned 200, ...).
        logging.warning(
            "BryteCube API returned zero view names - aborting without uploading "
            "to avoid marking the whole BryteCube collection for deletion."
        )
        return {}
 
    assets = [_build_collection_asset()] + _build_dataset_assets(names)
    return _upload_assets(assets, dry_run=dry_run)


def main():

    parser = argparse.ArgumentParser(description="Sync BryteCube views into the Datenprodukte (DNK) scheme.")
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Run the upload as a dataspot dry run (no data is changed; the response shows what would happen).",
    )
    args = parser.parse_args()
    
    response = upload_brytecube_datenprodukte_from_api(dry_run=args.dry_run)
    logging.info(f"Response:\n{json.dumps(response, indent=2, ensure_ascii=False)}")


if __name__ == "__main__":
    if config.logging_for_prod:
        logging.basicConfig(level=logging.INFO)
    else:
        logging.basicConfig(
            level=logging.INFO,
            format="%(asctime)s [%(levelname)s] %(filename)s:%(lineno)d %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S",
        )
    logging.info(f"=== CURRENT DATABASE: {config.database_name} ===")
    logging.info(f"Executing {__file__}...")
    main()
