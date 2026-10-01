import argparse
import json
import logging
import os
from typing import Any, Dict, List, Optional
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

# Disable proxies for the entire process
#NOTE: this needs to be enabled again to access dataspot
os.environ['no_proxy'] = 'brytecube.aue.wsu.bs.ch'
os.environ['HTTP_PROXY'] = ''
os.environ['HTTPS_PROXY'] = ''


DEFAULT_UPLOAD_FILE = os.path.join(os.path.dirname(__file__), "brytcube_test.json")
BRYTECUBE_VIEWS_API_URL = "https://brytecube.aue.wsu.bs.ch/BcOdata/WebApi/ResourceMetadata/AllViews/46ba9cdc-f416-421b-a38f-75715f6a1554"

# for "inCollection" value
BRYTECUBE_COLLECTION_PATH = ("Regierung und Verwaltung/Departement für Wirtschaft, Soziales und Umwelt/Amt für Umwelt und Energie (AUE)/BryteCube")


##### Questions: 
# - how to handle views that do not exist anymore
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
    """
    Fetch the list of BryteCube view metadata records from the BryteCube Web API.

    Authenticates with HTTP Basic Auth using the AUE_AD_USERNAME / AUE_AD_PASSWORD
    AD service account credentials (loaded from .env).

    Args:
        url: The BryteCube API endpoint to call.

    Returns:
        list: The raw view metadata objects, as returned by the API.

    Raises:
        ValueError: If AUE_AD_USERNAME/AUE_AD_PASSWORD are not set, or the response
            isn't a JSON list.
        HTTPError: If the request fails.
    """
    if not AUE_AD_USERNAME or not AUE_AD_PASSWORD:
        raise ValueError(
            "AUE_AD_USERNAME and AUE_AD_PASSWORD must be set (e.g. in .env) to call the BryteCube API"
        )

    logging.info(f"Fetching BryteCube views from {url} ...")

    ## NOTE: verity=False is only a quick and dirty fix, in the longrun a certificate is needed (from aue or it?)
    response = requests.get(url, auth=HttpNtlmAuth(username=username, password=password), verify=False)
    
    views = response.json()

    with open("test.json", "w", encoding="utf-8") as file:
        json.dump(views, file, indent=2, ensure_ascii=False)

    logging.info(f"Fetched {len(views)} view(s) from the BryteCube API")
    return views


def _extract_view_names(views: List[Dict[str, Any]]) -> List[str]:
    """
    Extract and de-duplicate the TY_LONGNAME field from each BryteCube view record.
    Entries missing TY_LONGNAME are skipped and logged.
    """
    names: List[str] = []
    seen = set()
    for view in views:
        name = (view.get("TY_LONGNAME") or "").strip()
        if not name:
            logging.warning(f"Skipping view with missing/blank TY_LONGNAME: {view}")
            continue
        if name in seen:
            logging.debug(f"Skipping duplicate view name within this batch: '{name}'")
            continue
        seen.add(name)
        names.append(name)
    return names


def _get_brytecube_collection_uuid(client: BaseDataspotClient, collection_path: str = BRYTECUBE_COLLECTION_PATH) -> str:
    """Resolve the UUID of the BryteCube collection from its business-key path."""
    path_elements = ["rest", config.database_name, "schemes", config.dnk_scheme_name]
    for folder in collection_path.split("/"):
        path_elements.append("collections")
        path_elements.append(folder)
    endpoint = url_join(*path_elements, leading_slash=True)

    collection = client._get_asset(endpoint)
    if not collection or not collection.get("id"):
        raise ValueError(f"BryteCube collection not found at path '{collection_path}'")
    return collection["id"]


def _get_existing_datasets(client: BaseDataspotClient, collection_uuid: str) -> Dict[str, str]:
    """Return {label: uuid} for Datasets already present directly in the given collection."""
    query = f"""
        SELECT id, label
        FROM dataset_view
        WHERE in_collection = '{collection_uuid}'
    """
    results = client.execute_query_api(sql_query=query)
    return {row["label"]: row["id"] for row in results if row.get("label") and row.get("id")}


def _remove_stale_datasets(client: BaseDataspotClient, existing_datasets: Dict[str, str], current_names: set) -> List[str]:
    """
    mark DELETENEW Datenprodukte that exist in the BryteCube collection
    but are no longer present in current_names (i.e. no longer returned by the BryteCube API).

    Args:
        existing_datasets: {label: uuid} of Datenprodukte currently in the collection.
        current_names: The set of view names currently returned by the BryteCube API.

    Returns:
        list: Labels of the Datenprodukte that were removed (or would be, in a dry run).
    """
    stale_labels = [label for label in existing_datasets if label not in current_names]

    if not stale_labels:
        logging.info("No stale Datenprodukte to remove - BryteCube collection already matches the API.")
        return []

    logging.info(f"{len(stale_labels)} Datenprodukt(e) no longer in the BryteCube API, removing: {stale_labels}")

    for label in stale_labels:
        uuid = existing_datasets[label]
        endpoint = url_join("rest", config.database_name, "datasets", uuid, leading_slash=True)

        logging.info(f"Marking Datenprodukt '{label}' (uuid={uuid}) for deletion review")
        client._mark_asset_for_deletion(endpoint)

    return stale_labels


def _build_dataset_assets(names: List[str], existing_labels: Optional[set] = None) -> List[Dict[str, Any]]:
    """
    Build bulk-upload asset dicts (new Datasets, no "id") for the given Datenprodukt names,
    skipping any name that already exists in the target collection.
    """
    existing_labels = existing_labels or set()
    assets = []
    for name in names:
        if name in existing_labels:
            logging.debug(f"Datenprodukt '{name}' already exists in BryteCube collection, skipping")
            continue
        assets.append({
            "_type": "Dataset",
            "label": name,
            "inCollection": BRYTECUBE_COLLECTION_PATH,
        })
    return assets


def _upload_assets(assets: List[Dict[str, Any]], operation: str = "ADD") -> Dict[str, Any]:
    """Push a list of asset dicts to the Datenprodukte (DNK) scheme via the bulk upload API."""
    if config.database_name != "test-aue-brytecube-api":
        logging.warning(
            f"config.database_name is '{config.database_name}', not the expected "
            "'test-aue-brytecube-api'. Double-check config.py before proceeding."
        )

    client = BaseDataspotClient(scheme_name=config.dnk_scheme_name, scheme_name_short=config.dnk_scheme_name_short)

    if not assets:
        logging.info("No assets to upload (everything already exists or nothing was found).")
        return {}

    logging.info(
        f"Uploading {len(assets)} asset(s) to scheme '{client.scheme_name}' in database "
        f"'{config.database_name}' (operation={operation}, dry_run={dry_run}, status={status})..."
    )

    response = client.bulk_create_or_update_assets(scheme_name=client.scheme_name, data=assets)

    errors = _extract_upload_errors(response)
    if errors:
        logging.error(f"Upload completed with {len(errors)} error(s):")
        for error in errors:
            logging.error(f"  - {error}")
    else:
        logging.info("Upload completed without errors.")

    return response


def upload_brytecube_datenprodukte_from_api(url: str = BRYTECUBE_VIEWS_API_URL) -> Dict[str, Any]:
    """
    Fetch BryteCube views from the BryteCube API and mirror them into the BryteCube
    collection in Dataspot: one Datenprodukt per view (named after TY_LONGNAME).

    Datenprodukte whose name already exists in the BryteCube collection are skipped
    on creation, so this can safely be re-run without creating duplicates.

    Args:
        mirror: Whether to remove Datenprodukte no longer present in the BryteCube API
                response. Defaults to True. Set to False to only create/update, never remove.
        force_delete_removed: If True, permanently delete stale Datenprodukte instead of
                marking them for deletion review (DELETENEW). Defaults to False.
    """
    views = fetch_brytecube_views(url)
    names = _extract_view_names(views)
    current_names = set(names)
    logging.info(f"Extracted {len(names)} unique Datenprodukt name(s) from TY_LONGNAME")

    client = BaseDataspotClient(scheme_name=config.dnk_scheme_name,
                                 scheme_name_short=config.dnk_scheme_name_short)
    collection_uuid = _get_brytecube_collection_uuid(client)
    existing_datasets = _get_existing_datasets(client, collection_uuid)
    logging.info(f"Found {len(existing_datasets)} existing Datenprodukt(e) in the BryteCube collection")

    assets = _build_dataset_assets(names, set(existing_datasets))
    logging.info(f"{len(assets)} new Datenprodukt(e) to create")

    response = _upload_assets(assets)

    if not current_names:
        # Safety net: never wipe out the whole collection just because the BryteCube
        # API returned nothing (e.g. transient error, auth failure that still returned
        # 200, or an empty page). Bail out instead of treating every existing
        # Datenprodukt as "no longer there".
        logging.warning(
            "BryteCube API returned zero view names - skipping removal step entirely "
            "to avoid deleting the whole BryteCube collection."
        )
    else:
        _remove_stale_datasets(client, existing_datasets, current_names)

    return response


# for test purposes
def upload_brytecube_datenprodukte_from_file(file_path: str = DEFAULT_UPLOAD_FILE) -> Dict[str, Any]:
    """
    Upload a hand/export-crafted BryteCube Datenprodukte JSON file to Dataspot.

    Items in the file that already carry an "id" are matched to the existing asset and
    updated; items without an "id" are created new. Kept for testing without access to
    the BryteCube API - see upload_brytecube_datenprodukte_from_api for the live source.
    """
    with open(file_path, "r", encoding="utf-8") as f:
        assets = json.load(f)

    logging.info(f"Loaded {len(assets)} asset(s) from {file_path}")
    return _upload_assets(assets)


def main():
    parser = argparse.ArgumentParser(
        description="Create AUE BryteCube Datenprodukte in Dataspot via the bulk upload API."
    )
    parser.add_argument("--source", default="api", choices=["api", "file"],
                         help="Where to read Datenprodukte from. 'api' (default) fetches views "
                              "from the BryteCube Web API and creates one Datenprodukt per view. "
                              "'file' uploads --file as-is (for testing without API access).")
    args = parser.parse_args()

    if args.source == "api":
        response = upload_brytecube_datenprodukte_from_api(
            url=args.url,
            operation=args.operation,
            dry_run=args.dry_run,
            status=args.status,
            mirror=not args.no_remove,
            force_delete_removed=args.force_delete_removed,
        )
    else:
        response = upload_brytecube_datenprodukte_from_file(
            file_path=args.file,
            operation=args.operation,
            dry_run=args.dry_run,
            status=args.status,
        )
    logging.info(f"Response:\n{json.dumps(response, indent=2, ensure_ascii=False)}")


""" if __name__ == "__main__":
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
    main() """

if __name__ == "__main__":
    print(AUE_AD_USERNAME)
    fetch_brytecube_views()
