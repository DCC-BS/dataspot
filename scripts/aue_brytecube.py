import argparse
import json
import logging
import os
import sys
from typing import Any, Dict, List
from dotenv import load_dotenv
import config
from src.clients.dnk_client import DNKClient # für Datenprodukte
from src.clients.tdm_client import TDMClient # für Datenbankobjekte
from src.common import requests
from requests_ntlm import HttpNtlmAuth
from src.clients.helpers import parse_iso_date_utc_midnight_ms
from collections import Counter
import re

load_dotenv()

AUE_AD_USERNAME = os.getenv("AUE_AD_USERNAME")
AUE_AD_PASSWORD = os.getenv("AUE_AD_PASSWORD")

REALM = "BS.CH"

username = f"{REALM}\\{AUE_AD_USERNAME}"
password = AUE_AD_PASSWORD

BRYTECUBE_VIEWS_API_URL = "https://brytecube.aue.wsu.bs.ch/BcOdata/WebApi/ResourceMetadata/AllViews/46ba9cdc-f416-421b-a38f-75715f6a1554"
BRYTECUBE_VIEW_DESCRIPTION_URL = "https://brytecube.aue.wsu.bs.ch/BcOdataApp/WebApi/ApplicationOdata/ViewDescription"

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

# technically these could be hardcoded?
SIMPLE_TYPES = {
    "System.Guid": ("uniqueidentifier", "STRING", {}),
    "System.Int16": ("smallint(5)", "INTEGER", {"minInclusive": -32768, "maxInclusive": 32767}),
    "System.Int32": ("int(10)", "INTEGER", {"minInclusive": -2**31, "maxInclusive": 2**31 - 1}),
    "System.Int64": ("bigint(19)", "INTEGER", {"minInclusive": -2**63, "maxInclusive": 2**63 - 1}),
    "System.DateTime": ("datetime", "DATETIME", {}),
    "System.Boolean": ("bit", "BOOLEAN", {}),
    "System.Double": ("float", "DECIMAL", {}),
    "System.Data.Entity.Spatial.DbGeometry": ("geometry", "BINARY", {}),
}

datenprodukte = []
datenbankobjekte = []
created_data_types = []

##### TODO: 
# - certificate?
# - error handling

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

####################################################### extract ###################################################################

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

####################################################### transform ###################################################################

def extract_relevant_views(views: List[Dict[str, Any]]) -> tuple[List[Dict[str, Any]], Dict[str, str]]:
    kept_views: List[Dict[str, Any]] = []
    label_by_id: Dict[str, str] = {} # to determine parent name

    for view in views:
        name = (view.get("TY_NAME") or "").strip()
        if not name.startswith(VIEW_NAME_PREFIX):
            logging.debug(f"Skipping view with TY_NAME '{name}' (does not start with '{VIEW_NAME_PREFIX}')")
            continue

        ly_ms_id = (view.get("LY_MS_ID") or "").strip()
        if ly_ms_id:
            logging.debug(f"Skipping entiry with TY_NAME '{name}', not a view")
            continue

        kept_views.append(view)

        ty_id = (view.get("TY_ID") or "").strip()
        if ty_id:
            label_by_id[ty_id] = name

    return [kept_views, label_by_id]

def generate_datenprodukte_asset(view: Dict[str, Any], longname_counts: Counter):
    longname = (view.get("TY_LONGNAME") or "").strip()
    name = (view.get("TY_NAME") or "").strip()

    description = (view.get("TY_DESCRIPTION") or "").strip()
    acronym = (view.get("TY_ACRONYM") or "").strip()
    creation_date = (view.get("TY_CREATED") or "").strip()
    last_updated = (view.get("TY_MODIFIED") or "").strip()

    # for the actual datenprodukte
    asset = {
        "_type": "Dataset",
        "inCollection": BRYTECUBE_COLLECTION_PATH,
        "actualLabel": longname
    }

    if longname_counts[longname] > 1:
        asset["label"] = f"{longname} ({name}) (AUE)"
    else:
        asset["label"] = f"{longname} (AUE)"

    asset["description"] = description
    asset["creationDate"] = parse_iso_date_utc_midnight_ms(creation_date)
    asset["lastUpdate"] = parse_iso_date_utc_midnight_ms(last_updated)

    if acronym:
        asset["tags"] = [acronym]

    datenprodukte.append(asset)

def generate_datenbankobjekte_asset(view: Dict[str, Any], label_by_id: Dict[str, str]):
    parent_id = (view.get("TY_PARENT") or "").strip()
    name = (view.get("TY_NAME") or "").strip()

    asset = ({
        "_type": "UmlClass",
        "label": name,
        "inCollection": BRYTECUBE_OBJECTS_PATH,
    })

    if parent_id:
        parent_name = label_by_id.get(parent_id)
        if parent_name:
            asset["subtypeOf"] = f"{BRYTECUBE_OBJECTS_PATH}/{parent_name}"
        else:
            logging.warning(
                f"View '{name}' has parent TY_ID '{parent_id}', which is not among the "
                f"uploaded views (filtered out or missing) - skipping subtypeOf."
            )

    datenbankobjekte.append(asset)

def generate_enumeration(enum_name: str, name: str, attr):
    
    datenbankobjekte.append({
        "_type" : "UmlEnumeration",
        "label" : enum_name,
        "inCollection" : BRYTECUBE_OBJECTS_PATH,
    })

    # NOTE: entriy names are not unique (see e.g. AKT_LANDKEY in VW_AKTEUR, countries Bhutan, Bolivien, Chile etc. )

    seen_entry_names = []
    for code in attr["CodeList"].values():

        entry_name = " ".join(code.get("TY_NAME").split()) # apparently contains new lines

        if entry_name in seen_entry_names:
            logging.info(f"Skipping {entry_name} (in view {name}), already exists\n")
            continue
        else:
            entry_title = " ".join(code.get("TY_LONGNAME").split())
            entry_description = code.get("TY_DESCRIPTION") or ""
            entry_tags = code.get("TY_ACRONYM") or ""

            enumeration = {
                "_type" : "UmlLiteral",
                "literalOf" : f"{BRYTECUBE_OBJECTS_PATH}/{enum_name}",
                "label" : entry_name,
                "title" : entry_title,
                "description" : entry_description,
            }

            if entry_tags:
                enumeration["tags"] = [entry_tags]

            datenbankobjekte.append(enumeration)

            seen_entry_names.append(entry_name)

def generate_data_type(attr):
    data_type = {
        "_type" : "UmlDatatype",
        "inCollection" : BRYTECUBE_OBJECTS_PATH,
    }

    m = re.match(r"System\.Nullable`1\[\[([^,\]]+)", attr["PocoType"])
    if m : 
        base_type = m.group(1)
    else: 
        base_type = attr["PocoType"].split(",")[0]

    bcAttribute = attr.get("BcAttribute") or {}

    if base_type in SIMPLE_TYPES:
        label, base, facets = SIMPLE_TYPES[base_type]
        data_type["baseType"] = base
        if facets:
            data_type["minInclusive"] = facets.get("minInclusive")
            data_type["maxInclusive"] = facets.get("maxInclusive")
    elif base_type == "System.String":
        length = bcAttribute.get("AT_LENGTH") or ""
        if length:
            label = f"nvarchar({length})" #nvarchar vs varchar?
            data_type["maxLength"] = length
        else: 
            label = "string"
        data_type["baseType"] = "STRING"
    elif base_type == "System.Decimal":
        length = bcAttribute.get("AT_LENGTH") or ""
        if length:
            precision = bcAttribute.get("AT_PRECISION") or ""
            if precision:
                label = f"decimal({length},{precision})"
                data_type["fractionDigits"] = precision
            else:
                label = f"decimal({length})"
            data_type["integerDigits"] = length
        else:
            label = "decimal"
        data_type["baseType"] = "DECIMAL"
    else:
        logging.warning(f"unknown base type in: {base_type}")
        label = base_type

    data_type["label"] = label
    
    if not data_type in created_data_types:
        datenbankobjekte.append(data_type)
        created_data_types.append(data_type)

    return label


def generate_attributes(attr, attribute_display_name_counts: Counter, longname_counts: Counter, name: str, longname: str):
    attribute_name = (attr.get("Name") or "").strip()

    datenprodukte_attribute = {
        "_type": "Composition",
        "composedOf" : f"/Datenbankobjekte/{BRYTECUBE_OBJECTS_PATH}/{name}/{attribute_name}",
        "actualLabel": attr["DisplayName"]
    }

    datenbankobjekte_attribute = {
        "_type": "UmlAttribute",
        "hasDomain": f"{BRYTECUBE_OBJECTS_PATH}/{name}",
        "label": attribute_name
    }
    
    if longname_counts[longname] > 1:
        datenprodukte_attribute["componentOf"] = f"{BRYTECUBE_COLLECTION_PATH}/{longname} ({name}) (AUE)"
    else: 
        datenprodukte_attribute["componentOf"] = f"{BRYTECUBE_COLLECTION_PATH}/{longname} (AUE)"

    if attribute_display_name_counts[attr["DisplayName"]] > 1:
        datenprodukte_attribute["label"] = f"{attr["DisplayName"]} ({attribute_name})"
    else:
        datenprodukte_attribute["label"] = attr["DisplayName"]

    bcAttribute = attr.get("BcAttribute") or {}

    attribute_description = (bcAttribute.get("TY_DESCRIPTION") or "").strip()
    attribute_acronym = (bcAttribute.get("TY_ACRONYM") or "")

    datenprodukte_attribute["description"] = attribute_description

    if attribute_acronym:
        datenprodukte_attribute["tags"] = [attribute_acronym]

    nullable = bcAttribute.get("AT_NULLABLE")

    if nullable == False:
        datenbankobjekte_attribute["required"] = "MANDATORY"
    else:
        datenbankobjekte_attribute["required"] = "OPTIONAL"

    datenbankobjekte_attribute["identifying"] = bcAttribute.get("AT_PRIMARY_KEY")

    # if the attribute has a code list, add those as enum
    if attr["CodeList"]:
        enum_name = f"{name} {attribute_name} Enum" #TODO: how to name the enums??
        generate_enumeration(enum_name, name, attr)
        datenbankobjekte_attribute["hasRange"] = f"{BRYTECUBE_OBJECTS_PATH}/{enum_name}"

    # if not, create data type (or use exsting data type)
    else:
        label = generate_data_type(attr)
        datenbankobjekte_attribute["hasRange"] = f"{BRYTECUBE_OBJECTS_PATH}/{label}"

    datenbankobjekte.append(datenbankobjekte_attribute)
    datenprodukte.append(datenprodukte_attribute)


def transform_bytecube_views(views: List[Dict[str, Any]]):

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

    kept_views, label_by_id = extract_relevant_views(views)
    longname_counts = Counter([(item.get("TY_LONGNAME") or "").strip() for item in kept_views])

    for view in kept_views:

        generate_datenprodukte_asset(view, longname_counts)
        generate_datenbankobjekte_asset(view, label_by_id)

        longname = (view.get("TY_LONGNAME") or "").strip() #TODO: wie schön machen??
        name = (view.get("TY_NAME") or "").strip()

        ty_id = (view.get("TY_ID") or "").strip()
        url = f"{BRYTECUBE_VIEW_DESCRIPTION_URL}/{ty_id}/46ba9cdc-f416-421b-a38f-75715f6a1554"
        viewDescription = extract_brytecube_data(url)

        for entity in viewDescription:
            attributes = list((entity.get("Attributes") or {}).values())
            attribute_display_name_counts = Counter([item.get("DisplayName").strip() for item in attributes])

            for attr in attributes:
                generate_attributes(attr, attribute_display_name_counts, longname_counts, name, longname)

############################################################ load ####################################################################

def upload_assets_to_dataspot(dry_run: bool = False):
    """
    Reconcile the BryteCube collection in the Datenprodukte (DNK) scheme via the bulk upload API.
 
    - New views are added, existing ones are updated (matched by label).
    - Datenprodukte previously uploaded by this script that are no longer in the upload are set to STALE_STATUS.
    """
    # NOTE: remove this once the script goes live
    if config.database_name != "test-aue-brytecube-api":
        logging.warning(
            f"config.database_name is '{config.database_name}', not the expected "
            "'test-aue-brytecube-api'. Double-check config.py before proceeding."
        )

    tdm_client = TDMClient()
    
    tdm_client.bulk_create_or_update_assets(
        scheme_name=tdm_client.scheme_name,
        data=datenbankobjekte,
        operation="REPLACE",
        status="PUBLISHED",
        on_delete=STALE_STATUS,
        agent_id=BRYTECUBE_AGENT_ID,
        dry_run=dry_run,
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

    if not views:
        logging.error("BryteCube API returned zero views - aborting.")
        sys.exit(1)
    
    transform_bytecube_views(views)

    with open("upload_datenbankobjekte.json", "w", encoding="utf-8") as file:
        json.dump(datenbankobjekte, file, indent=2, ensure_ascii=False)

    with open("upload_datenprodukte.json", "w", encoding="utf-8") as file:
        json.dump(datenprodukte, file, indent=2, ensure_ascii=False)

    upload_assets_to_dataspot(dry_run=args.dry_run)


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
