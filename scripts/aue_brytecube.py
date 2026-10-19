import argparse
import json
import logging
import os
from typing import Any, Dict, List
from dotenv import load_dotenv
import config
from src.clients.dnk_client import DNKClient # für Datenprodukte
from src.clients.tdm_client import TDMClient # für Datenbankobjekte
from src.common import requests
from requests_ntlm import HttpNtlmAuth
from src.clients.helpers import parse_iso_date_utc_midnight_ms

load_dotenv()

AUE_AD_USERNAME = os.getenv("AUE_AD_USERNAME")
AUE_AD_PASSWORD = os.getenv("AUE_AD_PASSWORD")

REALM = "BS.CH"

username = f"{REALM}\\{AUE_AD_USERNAME}"
password = AUE_AD_PASSWORD

BRYTECUBE_VIEWS_API_URL = "https://brytecube.aue.wsu.bs.ch/BcOdata/WebApi/ResourceMetadata/AllViews/46ba9cdc-f416-421b-a38f-75715f6a1554"
BRYTECUBE_VIEW_DESCRIPTION_URL = "https://brytecube.aue.wsu.bs.ch/BcOdataApp/WebApi/ApplicationOdata/ViewDescription/"

# for "inCollection" value
#TODO: make this variable, read value by uuid from dataspot
BRYTECUBE_COLLECTION_PATH = ("Regierung und Verwaltung/Departement für Wirtschaft, Soziales und Umwelt/Amt für Umwelt und Energie (AUE)/BryteCube")
BRYTECUBE_OBJECTS_PATH = ("Regierung und Verwaltung/Departement für Wirtschaft, Soziales und Umwelt/Amt für Umwelt und Energie (AUE)/BryteCube/API")

# Identifies this script as the owner of the Datenprodukte it uploads. With operation=REPLACE,
# only assets carrying this agentId are considered obsolete, so Datenprodukte created by hand
# (or by other scripts) in the BryteCube collection are never touched.
BRYTECUBE_AGENT_ID = "brytecube-sync"
 
# Status set on Datenprodukte that are no longer returned by the BryteCube API
# TODO: decide what to do, review to delete or archive
#STALE_STATUS = "DELETENEW"
STALE_STATUS = "ARCHIVEMETA"

VIEW_NAME_PREFIX = "VW_"

##### TODO: 
# - certificate?
# - more information on views via ty-id?
# - datenbankobjekte mit generieren? -> felder können potentiell aus viewDescription call geholt werden
# - was genau sind die einträge die keine views sind?
# - set some of them as geodaten?
# - entry names (see ..\composition)
# - gleicher longname bei unterschiedlichem name and vice versa -> wie lösen?


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


def extract_brytecube_data(url: str) -> List[Dict[str, Any]]:
    
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
 
    response = response.json()

    """ with open("test.json", "w", encoding="utf-8") as file:
        json.dump(views, file, indent=2, ensure_ascii=False) """

    return response


def transform_bytecube_views(views: List[Dict[str, Any]]) -> tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
    datenprodukte = []
    datenbankobjekte = []

    # for the brytecube collection itself
    parent_path, _, collection_label = BRYTECUBE_COLLECTION_PATH.rpartition("/")
    datenprodukte.append({
        "_type": "Collection",
        "label": collection_label,
        "inCollection": parent_path,
    })

    parent_path, _, collection_label = BRYTECUBE_OBJECTS_PATH.rpartition("/")
    datenbankobjekte.append({
        "_type" : "Collection",
        "label" : collection_label,
        "inCollection" : parent_path
    })

    # extract relevant fields
    seen = set() # to avoid duplicates; TODO: how to handle duplicates? datenprodukte vs datenbankobjekte?
    kept_views: List[Dict[str, Any]] = []
    label_by_id: Dict[str, str] = {} # to determine parent name
    for view in views:
        name = (view.get("TY_NAME") or "").strip()
        if not name.startswith(VIEW_NAME_PREFIX):
            logging.debug(f"Skipping view with TY_NAME '{name}' (does not start with '{VIEW_NAME_PREFIX}')")
            continue

        longname = (view.get("TY_LONGNAME") or "").strip()
        if not longname:
            logging.warning(f"Skipping view with missing/blank TY_LONGNAME: {view}")
            continue
        if name in seen or longname in seen:
            logging.warning(f"Duplicate TY_NAME '{name}' or TY_LONGNAME '{longname}' - keeping only the first occurrence")
            continue
        seen.add(name)
        seen.add(longname)
        kept_views.append(view)

        ty_id = (view.get("TY_ID") or "").strip()
        if ty_id:
            label_by_id[ty_id] = longname

    for view in kept_views:
        longname = (view.get("TY_LONGNAME") or "").strip()
        name = (view.get("TY_NAME") or "").strip()

        description = (view.get("TY_DESCRIPTION") or "").strip()
        if not description:
            logging.debug(f"View '{longname}' has no TY_DESCRIPTION")

        acronym = (view.get("TY_ACRONYM") or "").strip()
        if not acronym:
            logging.debug(f"View '{longname}' has no TY_ACRONYM")

        creation_date = (view.get("TY_CREATED") or "").strip()
        if not creation_date:
            logging.debug(f"View '{longname}' has no TY_CREATED")

        last_updated = (view.get("TY_MODIFIED") or "").strip()
        if not last_updated:
            logging.debug(f"View '{longname}' has no TY_MODIFIED")

        parent_id = (view.get("TY_PARENT") or "").strip()
        if parent_id:
            asset = {
                "_type": "Dataset",
                "label": longname + " (AUE)",
                "subsetOf": label_by_id[parent_id] + " (AUE)"
            }
        else:
            # for the actual datenprodukte
            asset = {
                "_type": "Dataset",
                "label": longname + " (AUE)",
                "inCollection": BRYTECUBE_COLLECTION_PATH,
            }


        if description:
            asset["description"] = description
        if acronym:
            asset["tags"] = [acronym] #im moment als schlüsselwort, evt als synonym?
        if creation_date:
            asset["creationDate"] = parse_iso_date_utc_midnight_ms(creation_date)
        if last_updated:
            asset["lastUpdate"] = parse_iso_date_utc_midnight_ms(last_updated)

        datenprodukte.append(asset)

        datenbankobjekte.append({
            "_type": "UmlClass",
            "label": name, #TODO: naming??
            "inCollection": BRYTECUBE_OBJECTS_PATH,
        })

        # extract more information on each view, like column names
        ty_id = (view.get("TY_ID") or "").strip() #redundancy? extract only once?

        if ty_id == "6850bef6-0087-4599-8e3b-d24678b676d0":
            url = BRYTECUBE_VIEW_DESCRIPTION_URL + ty_id + "/46ba9cdc-f416-421b-a38f-75715f6a1554"

            viewDescription = extract_brytecube_data(url)

            datenprodukt_id = "b8fb2ebc-428f-4e28-af09-06ab0520c545" 

            """ for entity in viewDescription:
                for attr in entity["Attributes"].values():
                    print((attr["Name"], attr["DisplayName"]))
                    # TODO wie funkt. das mit parents vs component of vs composed of?
                    assets.append({
                        "_type": "Composition",
                        "componentOf": "Spritzasbestkataster (AUE)",
                        "composedOf": "Spritzasbestkataster (AUE)",
                        "label": attr["DisplayName"]
                    }) """

    return [datenprodukte, datenbankobjekte]


def upload_assets_to_dataspot(datenprodukte: List[Dict[str, Any]], datenbankobjekte: List[Dict[str, Any]], dry_run: bool = False):
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
 
    dnk_client = DNKClient()
 
    logging.info(
        f"Uploading {len(datenprodukte)} asset(s) to scheme '{dnk_client.scheme_name}' in database "
        f"'{config.database_name}' (operation=REPLACE, onDelete={STALE_STATUS}, "
        f"agentId={BRYTECUBE_AGENT_ID}, dryRun={dry_run})"
    )
 
    dnk_client.bulk_create_or_update_assets(
        scheme_name=dnk_client.scheme_name,
        data=datenprodukte,
        operation="REPLACE",
        status="PUBLISHED",
        on_delete=STALE_STATUS,
        #on_insert="PUBLISHED", #default is "WORKING", set to "PUBLISHED" on prod
        agent_id=BRYTECUBE_AGENT_ID,
        dry_run=dry_run,
    )

    tdm_client = TDMClient()

    tdm_client.bulk_create_or_update_assets(
        scheme_name=tdm_client.scheme_name,
        data=datenbankobjekte,
        operation="REPLACE",
        status="PUBLISHED",
        on_delete=STALE_STATUS,
        #on_insert="PUBLISHED", #default is "WORKING", set to "PUBLISHED" on prod
        agent_id=BRYTECUBE_AGENT_ID,
        dry_run=dry_run,
    )

    # TODO: error handling
    """ errors = _extract_upload_errors(response)
    if errors:
        logging.error(f"Upload completed with {len(errors)} error(s):")
        for error in errors:
            logging.error(f"  - {error}")
    else:
        logging.info("Upload completed without errors.") """


def main():

    parser = argparse.ArgumentParser(description="Sync BryteCube views into the Datenprodukte (DNK) scheme.")
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Run the upload as a dataspot dry run (no data is changed; the response shows what would happen).",
    )
    args = parser.parse_args()

    views = extract_brytecube_data(BRYTECUBE_VIEWS_API_URL)
    datenprodukte, datenbankobjekte = transform_bytecube_views(views)

    if not datenprodukte:
        # Safety net: with operation=REPLACE, uploading the collection without any Datasets
        # would mark every Datenprodukt in it as obsolete. Never do that just because the
        # BryteCube API returned nothing (transient error, auth failure that still returned 200, ...).
        logging.warning(
            "BryteCube API returned zero views - aborting without uploading "
            "to avoid archiving the whole BryteCube collection."
        )
        return {}

    upload_assets_to_dataspot(datenprodukte, datenbankobjekte, dry_run=args.dry_run)


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
