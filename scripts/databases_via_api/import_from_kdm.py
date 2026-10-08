import config
import requests
import logging
import json
from src.clients.tdm_client import TDMClient
from src.clients.dnk_client import DNKClient
from src.dataspot_auth import DataspotAuth
from typing import List, Dict, Any

from src.kdm_api import KdmClient

from markdownify import markdownify as md
import html

# Docs: https://kdm.test.bs.ch/api/config/swagger-ui/index.html#/

# Notes to self:
#
# Name of test-db is "test-kdm-api"
# This is in an unfinished state, but I'm waiting for feedback before I can continue - so I commit this in this state
#  for now.
# See different FIXME and TODOs.
# Logging needs to be unified (currently is at the top and in __main__.
# Regarding "ETL": ET is done, L is missing.
# Open question: Do we use "name" or "label" for the label in dataproducts. name is unclean ("adressePlzOrt"), label is
#  clean ("Adresse PLZ & Ort") but might be missing or duplicate. Waiting for Feedback from Dani H.
# Don't forget to publish and write-protect the folders.
# Don't forget to update Datenbankobjekte-YAML.
# Connector can probably be removed and all DBOs deleted (check with Jonas). If so, clean everything up cleanly:
#  - Remove user from SQL-Server
#  - Remove tufin rule
#  - Remove code
#  - Remove assets from dataspot

# --- CONFIGURATION ---
KDM_API_URL = "https://kdm.bs.ch/api/config"
kdm_client = KdmClient()

_TDM_KDM_COLLECTION_UUID = '839208cf-9535-41ce-a1d2-cb52036d7dcc'
_PHYSICAL_MODEL_LABEL = 'Physisches Modell'
_LOGICAL_MODEL_LABEL = 'Logisches Modell'

_DNK_KDM_COLLECTION_UUID = '1be154ad-e86a-4aed-8c8b-4e07ceb370cb' # CHANGE ME - THIS IS FOR TEST-DB ONLY

# TODO: uncomment these once we implement Load step
#STATUS_ON_CREATE = 'PUBLISHED'
#STATUS_ON_UPDATE = 'PUBLISHED'
#STATUS_ON_DELETE = None # None: Delete directly

# --- SETUP ---
dataspot_auth = DataspotAuth()

tdm_client = TDMClient()
_tdm_all_kdm_collections = requests.get(f"{config.base_url}/api/{config.database_name}/collections/{_TDM_KDM_COLLECTION_UUID}/download?mediaType=application%2Fjson&format=JSON&attachment=true&language=de&v=3&baseTenant=true&downloadName=Metadaten+Export&downloadKey=download.json&resourceTypes=Collection", headers=dataspot_auth.get_headers()).json()
_tdm_kdm_collection_path = [item['inCollection'] + '/' + item['label'] for item in _tdm_all_kdm_collections if item['_type'] == "Collection" and item['id'] == _TDM_KDM_COLLECTION_UUID][0]
PHYSICAL_MODEL_PATH = [item['inCollection'] + '/' + item['label'] for item in _tdm_all_kdm_collections if item['_type'] == 'Collection' and item.get('label') == _PHYSICAL_MODEL_LABEL][0]
LOGICAL_MODEL_PATH = [item['inCollection'] + '/' + item['label'] for item in _tdm_all_kdm_collections if item['_type'] == 'Collection' and item.get('label') == _LOGICAL_MODEL_LABEL][0]

dnk_client = DNKClient()
_dnk_all_kdm_collections = requests.get(f"{config.base_url}/api/{config.database_name}/collections/{_DNK_KDM_COLLECTION_UUID}/download?mediaType=application%2Fjson&format=JSON&attachment=true&language=de&v=3&baseTenant=true&downloadName=Metadaten+Export&downloadKey=download.json&resourceTypes=Collection", headers=dataspot_auth.get_headers()).json()
_dnk_kdm_incollection_path = [item['inCollection'] for item in _dnk_all_kdm_collections if item['_type'] == "Collection" and item['id'] == _DNK_KDM_COLLECTION_UUID][0]
_dnk_kdm_collection_label = [item['label'] for item in _dnk_all_kdm_collections if item['_type'] == "Collection" and item['id'] == _DNK_KDM_COLLECTION_UUID][0]
DNK_KDM_COLLECTION_PATH = f"{_dnk_kdm_incollection_path}/{_dnk_kdm_collection_label}"

# Logging configuration
# TODO: Not sure how we want to implement logging so that it works system-wide
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


def _html_to_markdown(html_string: str | None) -> str:
    if not html_string:
        return ""

    # 1. Convert HTML to Markdown
    # we use strip=True to remove unnecessary whitespace around tags
    markdown_text = md(html_string, strip=['span']).strip()

    # 2. Handle the &nbsp; entities
    # markdownify handles most, but to be absolutely sure about &nbsp;
    # and to clean up double newlines:
    markdown_text = html.unescape(markdown_text)
    markdown_text = markdown_text.replace('\u00a0', ' ')

    # 3. Fine-tuning the line breaks
    # HTML <p> tags often result in double newlines (\n\n)
    # We ensure the formatting matches your requirement exactly
    markdown_text = markdown_text.replace('\n\n\n', '\n\n').strip()
    return markdown_text


def extract_from_kdm() -> Dict[str, List[Dict[str, Any]]]:
    """
    1. EXTRACT: Reads data from the "Kantonaler Datenmarkt" (KDM).
    """
    logger.info("Starting extraction from KDM...")

    data = {"logical_entities": [],
            "logical_attributes": [],
            "physical_entities": [],
            "physical_attributes": [],
    }

    try:
        response = kdm_client.get(KDM_API_URL + "/modell/logisch")
        response.raise_for_status()

        data_logical = response.json()

        data["logical_entities"] = data_logical["entitaeten"]
        data["logical_attributes"] = data_logical["attribute"]

        logger.info(f"Successfully extracted {len(data['logical_entities'])} logical entities and {len(data['logical_attributes'])} logical attributes from KDM.")
    except requests.exceptions.RequestException as e:
        logger.error(f"Error reading logical model from KDM: {e}")
        return dict()

    try:
        response = kdm_client.get(KDM_API_URL + "/modell/physisch")
        response.raise_for_status()

        data_physical = response.json()

        data["physical_entities"] = data_physical["entities"]
        data["physical_attributes"] = data_physical["attributes"]

        logger.info(f"Successfully extracted {len(data['physical_entities'])} physical entities and {len(data['physical_attributes'])} physical attributes from KDM.")
    except requests.exceptions.RequestException as e:
        logger.error(f"Error reading physical model from KDM: {e}")
        return dict()

    return data


def transform_to_datenbankobjekte_json(raw_data: Dict[str, List[Dict[str, Any]]]) -> List[Dict[str, Any]]:
    """
    2a. TRANSFORM: Formats the data as JSON suitable for the Datenbankobjekte Catalog.
    """
    logger.info("Transforming data into JSON format for the Datenbankobjekte catalog...")
    transformed_data = []

    ### Physisches Modell ###
    physical_entities: List[Dict[str, Any]] = raw_data['physical_entities']
    physical_entities.sort(key=lambda d: d['id'])

    physical_entity_names_by_id = {item['id']: item['name'] for item in physical_entities}

    physical_attributes = raw_data['physical_attributes']
    physical_attributes.sort(key=lambda d: d['id'])

    # 1. Collection
    item = {
        "_type": "Collection",
        "label": f"{_PHYSICAL_MODEL_LABEL}",
        "inCollection": _tdm_kdm_collection_path,
    }
    transformed_data.append(item)

    # 2. UmlClass
    for entity in physical_entities:
        item = {
            "_type": "UmlClass",
            "label": entity['name'],
            "stereotype": "kdm_physical_entity",
            "inCollection": PHYSICAL_MODEL_PATH,
            "physicalName": entity['name'],
            "kdm_id": entity['id'],
        }
        transformed_data.append(item)

    # 3. UmlDatatype
    physical_datatypes = sorted(list(set([item['datentyp'] for item in physical_attributes])))
    for datatype in physical_datatypes:
        item = {
            "_type": "UmlDatatype",
            "label": datatype,
            "inCollection": PHYSICAL_MODEL_PATH,
        }
        transformed_data.append(item)

    # 4. UmlAttribute
    for attribute in physical_attributes:
        item = {
            "_type": "UmlAttribute",
            "hasDomain": f"{PHYSICAL_MODEL_PATH}/{physical_entity_names_by_id[attribute['entityId']]}",
            "label": attribute['name'],
            "hasRange": f"{PHYSICAL_MODEL_PATH}/{attribute['datentyp']}",
            "order": attribute['id'],
            "physicalName": attribute['name'],
        }
        transformed_data.append(item)

    # 5. Deployment
    for entity in physical_entities:
        item = {
            "_type": "Deployment",
            "deploymentOf": f"{PHYSICAL_MODEL_PATH}/{entity['name']}",
            "deployedIn": "/Systeme/Kantonaler Datenmarkt (KDM)",
        }
        transformed_data.append(item)

    ### Logisches Modell ###
    logical_entities: List[Dict[str, Any]] = raw_data["logical_entities"]
    logical_entities.sort(key=lambda d: d["id"])

    logical_entity_names_by_id = {item['id']: item['name'] for item in logical_entities}

    logical_attributes = raw_data['logical_attributes']
    logical_attributes.sort(key=lambda d: d["id"])

    logical_attributes_by_id = {item['id']: item for item in logical_attributes}

    # 1. Collection
    item = {
        "_type": "Collection",
        "label": f"{_LOGICAL_MODEL_LABEL}",
        "inCollection": _tdm_kdm_collection_path,
    }
    transformed_data.append(item)

    # 2. UmlClass
    for entity in logical_entities:
        item = {
            "_type": "UmlClass",
            "label": entity['name'],
            "stereotype": "kdm_logical_entity",
            "inCollection": LOGICAL_MODEL_PATH,
            "physicalName": entity['name'],
            "kdm_id": entity['id'],
        }
        if 'base' in entity:
            item['subtypeOf'] = f"{LOGICAL_MODEL_PATH}/{entity['base']}"
        transformed_data.append(item)

    # 3. UmlDatatype
    logical_datatypes = sorted(list(set([item['datentyp'] for item in logical_attributes])))
    for datatype in logical_datatypes:
        if datatype in logical_entity_names_by_id.values():
            continue
        item = {
            "_type": "UmlDatatype",
            "label": datatype,
            "inCollection": LOGICAL_MODEL_PATH,
        }
        transformed_data.append(item)

    # 4. UmlAttribute
    for attribute in logical_attributes:
        item = {
            "_type": "UmlAttribute",
            "hasDomain": f"{LOGICAL_MODEL_PATH}/{logical_entity_names_by_id[attribute['entitaetId']]}",
            "label": attribute['name'],
            "hasRange": f"{LOGICAL_MODEL_PATH}/{attribute['datentyp']}",
            "order": attribute['id'],
            "physicalName": attribute['name'],
            "identifying": False,
            "cardinality" : "ONE",
        }
        if attribute['isPk']:
            item['identifying'] = True
        if attribute['isCollection']:
            item['cardinality'] = "MANY"
        transformed_data.append(item)

    # 5. Derivation
    for physical_attribute in physical_attributes:
        if physical_attribute.get('logicalAttributeIds') is None:
            continue
        for logical_attribute_id in physical_attribute['logicalAttributeIds']:
            physical_entity_label = physical_entity_names_by_id[physical_attribute['entityId']]
            physical_attribute_label = physical_attribute['name']

            logical_attribute = logical_attributes_by_id[logical_attribute_id]
            logical_entity_label = logical_entity_names_by_id[logical_attribute['entitaetId']]
            logical_attribute_label = logical_attribute['name']

            item = {
                "_type": "Derivation",
                "derivedFrom": f"{PHYSICAL_MODEL_PATH}/{physical_entity_label}/{physical_attribute_label}",
                "derivedTo": f"{LOGICAL_MODEL_PATH}/{logical_entity_label}/{logical_attribute_label}",
            }
            transformed_data.append(item)

    # 6. Deployment
    for logical_entity in logical_entities:
        item = {
            "_type": "Deployment",
            "deploymentOf": f"{LOGICAL_MODEL_PATH}/{logical_entity['name']}",
            "deployedIn": "/Systeme/Kantonaler Datenmarkt (KDM)",
        }
        transformed_data.append(item)


    with open("kdm_import_datenbankobjekte.json", "w", encoding="UTF-8") as f:
        f.write(json.dumps(transformed_data, ensure_ascii=False, indent=4))

    return transformed_data


def transform_to_datenprodukte_json(raw_data: Dict[str, List[Dict[str, Any]]]) -> List[Dict[str, Any]]:
    """
    2b. TRANSFORM: Formats the data as JSON suitable for the Datenprodukte Catalog.
    """
    logger.info("Transforming data into JSON format for the Datenprodukte catalog...")
    transformed_data = []

    logical_entities: List[Dict[str, Any]] = raw_data["logical_entities"]
    logical_entities.sort(key=lambda d: d["id"])

    logical_entity_names_by_id = {item['id']: item['name'] for item in logical_entities}

    logical_attributes = raw_data['logical_attributes']
    logical_attributes.sort(key=lambda d: d["id"])

    # 1. Collection
    item = {
        "_type": "Collection",
        "label": _dnk_kdm_collection_label,
        "inCollection": _dnk_kdm_incollection_path,
    }
    transformed_data.append(item)

    # 2. Dataset
    for entity in logical_entities:
        item = {
            "_type": "Dataset",
            "label": f"{entity['name']} (KDM)",
            "order": entity['id'],
            "inCollection": DNK_KDM_COLLECTION_PATH,
        }
        transformed_data.append(item)

    # 3. Composition
    for attribute in logical_attributes:
        logical_entity_name = logical_entity_names_by_id[attribute['entitaetId']]
        logical_attribute_name = attribute['name']
        item = {
            "_type": "Composition",
            "componentOf": f"{logical_entity_name} (KDM)",
            "label": attribute.get('label', logical_attribute_name), # FIXME: I think the Label must not be empty. This is a fallback - but introduces inconsisteny with the actual dataproduct and should thus not be pushed! I just did this so I can debug code below.
            "composedOf": f"/Datenbankobjekte/{LOGICAL_MODEL_PATH}/{logical_entity_name}/{logical_attribute_name}",
            "order": attribute['id'],
            "title": logical_attribute_name,
            "description": "",
        }
        if 'beschreibung' in attribute:
            item['description'] = _html_to_markdown(attribute['beschreibung'])

        transformed_data.append(item)

    # 4. Deployment
    for entity in logical_entities:
        item = {
            "_type": "Deployment",
            "deploymentOf": f"{logical_entity_names_by_id[entity['id']]} (KDM)",
            "deployedIn": "/Systeme/Kantonaler Datenmarkt (KDM)",
        }
        transformed_data.append(item)

    with open("kdm_import_datenprodukte.json", "w", encoding="UTF-8") as f:
        f.write(json.dumps(transformed_data, ensure_ascii=False, indent=4))

    return transformed_data


def load_into_catalog(datenbankobjekte_json: List[Dict[str, Any]], datenprodukte_json: List[Dict[str, Any]]) -> bool:
    """
    3. LOAD: Imports the formatted data into the Data Catalog.
    """
    if not datenbankobjekte_json or not datenprodukte_json:
        logger.warning("No data available for import.")
        return False

    #logger.info(f"Importing {len(json_data)} entries into the data catalog...")

    # 1. Upload Datenbankobjekte: ADD (Add new Datenbankobjekte)
    # TODO: Implement me
    # https://datenkatalog.bs.ch/api/test-kdm-api/schemes/d5acd695-03fb-401f-8208-6210edd244f4/upload?operation=ADD&v=3&onInsert=PUBLISHED&onUpdate=PUBLISHED&dryRun=true

    # 2. Upload Datenprodukte: UPDATE
    # TODO: Implement me
    # https://datenkatalog.bs.ch/api/test-kdm-api/schemes/0f16581d-ddff-4815-a423-3628baa326cc/upload?operation=REPLACE&v=3&onInsert=PUBLISHED&onUpdate=PUBLISHED&dryRun=true

    # 3. Upload Datenbankobjekte: UPDATE (Delete Datenbankobjekte that no longer exist)
    # TODO: Implement me
    # https://datenkatalog.bs.ch/api/test-kdm-api/schemes/d5acd695-03fb-401f-8208-6210edd244f4/upload?operation=REPLACE&v=3&onInsert=PUBLISHED&onUpdate=PUBLISHED&dryRun=true

    try:
        #headers = {"Authorization": f"Bearer {API_TOKEN}", "Content-Type": "application/json"}
        # Depending on the API: Single POST requests or a bulk import
        #response = requests.post(CATALOG_API_URL, headers=headers, json=json_data, timeout=60)
        #response.raise_for_status()

        logger.info("Import into the data catalog completed successfully.")
        return True
    except requests.exceptions.RequestException as e:
        logger.error(f"Error importing into the data catalog: {e}")
        return False


def main():
    """
    Main control for the ETL process.
    """
    logger.info("--- ETL Process Start: KDM -> Data Catalog ---")

    # Step 1: Extract
    raw_data = extract_from_kdm()

    if raw_data:
        # Step 2: Transform
        datenbankobjekte_json = transform_to_datenbankobjekte_json(raw_data)
        datenprodukte_json = transform_to_datenprodukte_json(raw_data)

        # Step 3: Load
        #success = load_into_catalog(datenbankobjekte_json=datenbankobjekte_json, datenprodukte_json=datenprodukte_json)
        success = False

        if success:
            logger.info("ETL process finished successfully.")
        else:
            logger.error("ETL process failed during the data loading phase.")
    else:
        logger.error("ETL process aborted: No data received from KDM.")


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(filename)s:%(lineno)d %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
        force=True,
        )
    main()
