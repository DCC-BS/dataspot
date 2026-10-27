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
from collections import Counter
import re

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
# - error handling
# - clean and comment code
# - immer description, falls null leerer string


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
    created_data_types = [] # for else branch of data types

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

    longname_counts = Counter([item["TY_LONGNAME"] for item in kept_views])

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

        # for the actual datenprodukte
        asset = {
            "_type": "Dataset",
            "inCollection": BRYTECUBE_COLLECTION_PATH,
            "actualLabel": longname
        }

        if longname_counts[longname] > 1:
            asset["label"] = longname + " (" + name + ") (AUE)"
        else:
            asset["label"] = longname + " (AUE)"

        if description:
            asset["description"] = description
        if acronym:
            asset["tags"] = [acronym] #im moment als schlüsselwort, evt als synonym?
        if creation_date:
            asset["creationDate"] = parse_iso_date_utc_midnight_ms(creation_date)
        if last_updated:
            asset["lastUpdate"] = parse_iso_date_utc_midnight_ms(last_updated)

        datenprodukte.append(asset)

        asset = ({
            "_type": "UmlClass",
            "label": name,
            "inCollection": BRYTECUBE_OBJECTS_PATH,
        })

        if parent_id:
            asset["subtypeOf"] = BRYTECUBE_OBJECTS_PATH  + "/" + label_by_id[parent_id]

        datenbankobjekte.append(asset)

        ############ Attributes ##########################################################################

        ty_id = (view.get("TY_ID") or "").strip() #redundancy? extract only once?

        url = BRYTECUBE_VIEW_DESCRIPTION_URL + ty_id + "/46ba9cdc-f416-421b-a38f-75715f6a1554"

        viewDescription = extract_brytecube_data(url)

        for entity in viewDescription:
            attribute_display_name_counts = Counter([item["DisplayName"] for item in entity["Attributes"].values()])
            for attr in entity["Attributes"].values():

                attribute = {
                    "_type": "UmlAttribute",
                    "hasDomain": BRYTECUBE_OBJECTS_PATH + "/" + name,
                    "label": attr["Name"]
                }

                if attr["BcAttribute"]:
                    # NOTE: also get()? and strip()?
                    nullable = attr["BcAttribute"]["AT_NULLABLE"]
                    primary_key = attr["BcAttribute"]["AT_PRIMARY_KEY"]

                    if not nullable:
                        attribute["required"] = "MANDATORY"

                    if primary_key:
                        attribute["identifying"] = True

                # if the attribute has a code list, add those as enum
                if attr["CodeList"]:

                    #possible_entries = [" ".join(code["TY_NAME"].split()) for code in attr["CodeList"].values()]

                    # because the entries are not f-ing unique???? TODO: how to handle this? (see e.g. AKT_LANDKEY in VW_AKTEUR, countries Bhutan, Bolivien, Chile etc. )
                    # TODO: write Luca
                    possible_entries = list(dict.fromkeys(
                        " ".join(code["TY_NAME"].split()) for code in attr["CodeList"].values()
                    ))

                    enum_name = name + " " + attr["Name"] + " Enum" #TODO: how to name the enums??

                    datenbankobjekte.append({
                        "_type" : "UmlEnumeration",
                        "label" : enum_name,
                        "inCollection" : BRYTECUBE_OBJECTS_PATH,
                    })

                    for entry in possible_entries:

                        # TODO: add longname of code as "title", add description, acronym
                        datenbankobjekte.append({
                            "_type" : "UmlLiteral",
                            "literalOf" : BRYTECUBE_OBJECTS_PATH + "/" + enum_name,
                            "label" : entry,
                        })

                    attribute["hasRange"] = BRYTECUBE_OBJECTS_PATH + "/" + enum_name

                # if not, create data type (or use exsting data type)
                else:

                    data_type = {
                        "_type" : "UmlDatatype",
                        "inCollection" : BRYTECUBE_OBJECTS_PATH,
                    }

                    m = re.match(r"System\.Nullable`1\[\[([^,\]]+)", attr["PocoType"])
                    if m : 
                        base_type = m.group(1)
                    else: 
                        base_type = attr["PocoType"].split(",")[0]

                    if base_type == "System.String":
                        if attr["BcAttribute"]:
                            if attr["BcAttribute"]["AT_LENGTH"]:
                                length = attr["BcAttribute"]["AT_LENGTH"]
                                label = f"nvarchar({length})" #nvarchar vs varchar?

                                data_type["maxLength"] = length
                            else: 
                                label = "string"
                        else:
                            label = "string"
                        data_type["baseType"] = "STRING"
                    elif base_type == "System.Guid":
                        label = "uniqueidentifier"
                        data_type["baseType"] = "STRING"
                    elif base_type == "System.Int16":
                        label = "smallint(5)"
                        data_type["baseType"] = "INTEGER"
                        data_type["minInclusive"] = -32768.0
                        data_type["maxInclusive"] = 32767.0
                    elif base_type == "System.DateTime":
                        label = "datetime"
                        data_type["baseType"] = "DATETIME"
                    elif base_type == "System.Int32":
                        label = "int(10)"
                        data_type["baseType"] = "INTEGER"
                        data_type["minInclusive"] = -2147483648
                        data_type["maxInclusive"] = 2147483647
                    elif base_type == "System.Data.Entity.Spatial.DbGeometry":
                        label = "geometry"
                        data_type["baseType"] = "BINARY"
                    elif base_type == "System.Decimal":
                        if attr["BcAttribute"]:
                            if attr["BcAttribute"]["AT_LENGTH"]:
                                length = attr["BcAttribute"]["AT_LENGTH"]
                                if attr["BcAttribute"]["AT_PRECISION"]:
                                    precision = attr["BcAttribute"]["AT_PRECISION"]
                                    label = f"decimal({length},{precision})"
                                    data_type["fractionDigits"] = precision
                                else:
                                    label = f"decimal({length})"
                                data_type["integerDigits"] = length
                            else:
                                label = "decimal"
                        else: 
                            label = "decimal"
                        data_type["baseType"] = "DECIMAL"
                    elif base_type == "System.Boolean":
                        label = "bit"
                        data_type["baseType"] = "BOOLEAN"
                    elif base_type == "System.Int64":
                        label = "bigint(19)"
                        data_type["baseType"] = "INTEGER"
                        data_type["minInclusive"] = -9223372036854775808
                        data_type["maxInclusive"] = 9223372036854775807
                    elif base_type == "System.Double":
                        label = "float"
                        data_type["baseType"] = "DECIMAL"
                    else:
                        logging.warning(f"unknown base type in {name}, {attr["Name"]}: {base_type}")
                        label = base_type

                    data_type["label"] = label
                    attribute["hasRange"] = BRYTECUBE_OBJECTS_PATH + "/" + label
                    
                    if not data_type in created_data_types:
                        datenbankobjekte.append(data_type)
                        created_data_types.append(data_type)

                datenbankobjekte.append(attribute)

                ############# datenprodukte attribute ######################################

                attribute = {
                    "_type": "Composition",
                    "composedOf" : "/Datenbankobjekte/" + BRYTECUBE_OBJECTS_PATH + "/" + name + "/" + attr["Name"],
                    "actualLabel": attr["DisplayName"]
                }

                if longname_counts[longname] > 1:
                    attribute["componentOf"] = BRYTECUBE_COLLECTION_PATH + "/" + longname + " (" + name + ") (AUE)"
                else: 
                    attribute["componentOf"] = BRYTECUBE_COLLECTION_PATH + "/" + longname + " (AUE)"

                if attribute_display_name_counts[attr["DisplayName"]] > 1:
                    attribute["label"] = attr["DisplayName"] + " (" + attr["Name"] + ")"
                else:
                    attribute["label"] = attr["DisplayName"]

                if attr["BcAttribute"]:
                    # NOTE: also get()? and strip()?
                    attribute_description = attr["BcAttribute"]["TY_DESCRIPTION"]
                    attribute_acronym = attr["BcAttribute"]["TY_ACRONYM"]

                    if attribute_description:
                        attribute["description"] = attribute_description

                    if attribute_acronym:
                        attribute["tags"] = [attribute_acronym]

                datenprodukte.append(attribute)

    with open("upload.json", "w", encoding="utf-8") as file:
        json.dump(datenbankobjekte, file, indent=2, ensure_ascii=False)

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

    #TODO: sleep? how long does the api need to react? (datenbankobjekte werden beim upload nocht gefunden)

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
