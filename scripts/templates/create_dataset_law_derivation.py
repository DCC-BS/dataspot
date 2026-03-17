"""
One-off script: create disposable Dataset and ReferenceObject/ReferenceValue,
link them via Derivation (qualifier LAWFUL_BASIS), then delete both collections.
"""

import datetime
import logging
import uuid

import config
from src.clients.dnk_client import DNKClient
from src.clients.helpers import url_join
from src.clients.law_client import LAWClient
from src.common import requests_post
from src.dataspot_dataset import OGDDataset


def _namespace() -> str:
    return f"tmp-{datetime.datetime.now(datetime.timezone.utc).strftime('%Y%m%d%H%M%S')}-{uuid.uuid4().hex[:8]}"


def _create_collection_in_scheme(client, scheme_name: str, label: str) -> dict:
    endpoint = f"/rest/{config.database_name}/schemes/{scheme_name}/collections"
    data = {"_type": "Collection", "label": label}
    result = client._create_asset(endpoint=endpoint, data=data, status="PUBLISHED")
    logging.info("Created collection id=%s label=%s in scheme=%s", result.get("id"), label, scheme_name)
    return result


def _create_derivation(dataset_id: str, reference_value_id: str, auth_headers: dict) -> dict:
    url = f"{config.base_url}/rest/{config.database_name}/derivations"
    payload = {
        "_type": "Derivation",
        "derivedTo": dataset_id,
        "derivedFrom": reference_value_id,
        "qualifier": "LAWFUL_BASIS",
    }
    response = requests_post(url=url, json=payload, headers=auth_headers)
    response.raise_for_status()
    result = response.json()
    logging.info(
        "Created derivation id=%s resourceId=%s derivedFrom=%s qualifier=LAWFUL_BASIS",
        result.get("id"),
        dataset_id,
        reference_value_id,
    )
    return result


def _delete_collection(client, collection_id: str) -> None:
    endpoint = f"/rest/{config.database_name}/collections/{collection_id}"
    try:
        client._delete_asset(endpoint, force_delete=True)
        logging.info("Deleted collection id=%s", collection_id)
    except Exception as e:
        logging.error("Failed to delete collection id=%s: %s", collection_id, e)


def main() -> None:
    logging.info("Starting disposable dataset-law derivation script")
    namespace = _namespace()

    dnk_client = DNKClient()
    law_client = LAWClient()

    dataset_collection_id = None
    law_collection_id = None

    try:
        # 1. Create collection in Datenprodukte and Dataset inside it
        dataset_collection = _create_collection_in_scheme(
            dnk_client, config.dnk_scheme_name, f"{namespace}-dataset-collection"
        )
        dataset_collection_id = dataset_collection.get("id")
        if not dataset_collection_id:
            raise ValueError("Collection create response missing id")

        dataset_obj = OGDDataset(
            name=f"{namespace} Test Dataset",
            datenportal_identifikation=f"tmp-{namespace}",
        )
        dataset_payload = dataset_obj.to_json()
        dataset_payload["inCollection"] = dataset_collection_id
        dataset_creation_endpoint = url_join(
            "rest",
            config.database_name,
            "collections",
            dataset_collection_id,
            "datasets",
            leading_slash=True,
        )
        dataset = dnk_client._create_asset(
            endpoint=dataset_creation_endpoint, data=dataset_payload, status="PUBLISHED"
        )
        dataset_id = dataset.get("id")
        if not dataset_id:
            raise ValueError("Dataset create response missing id")
        logging.info("Created dataset id=%s", dataset_id)

        # 2. Create collection in Gesetzessammlungen and ReferenceObject + ReferenceValue inside it
        law_collection = _create_collection_in_scheme(
            law_client, config.law_scheme_name, f"{namespace}-law-collection"
        )
        law_collection_id = law_collection.get("id")
        if not law_collection_id:
            raise ValueError("Law collection create response missing id")

        ref_object_payload = {
            "_type": "ReferenceObject",
            "label": f"{namespace} Test Law",
            "description": "",
            "title": "Disposable law for derivation test",
            "customProperties": {"systematic_number": f"9999.IT.{namespace}"},
        }
        ref_object = law_client.create_reference_object(
            collection_uuid=law_collection_id, data=ref_object_payload, status="PUBLISHED"
        )
        ref_object_id = ref_object.get("id")
        if not ref_object_id:
            raise ValueError("ReferenceObject create response missing id")
        logging.info("Created ReferenceObject id=%s", ref_object_id)

        ref_value_payload = {
            "_type": "ReferenceValue",
            "timeSeries": [
                {
                    "code": "§ 1",
                    "shortText": "Test paragraph",
                    "validFrom": -2208988800000,
                    "validTo": 32503593600000,
                }
            ],
        }
        ref_value = law_client.create_reference_value(
            law_id=ref_object_id, data=ref_value_payload, status="PUBLISHED"
        )
        ref_value_id = ref_value.get("id")
        if not ref_value_id:
            raise ValueError("ReferenceValue create response missing id")
        logging.info("Created ReferenceValue id=%s", ref_value_id)

        # 3. Create Derivation from Dataset to ReferenceValue with qualifier LAWFUL_BASIS
        derivation = _create_derivation(
            dataset_id=dataset_id,
            reference_value_id=ref_value_id,
            auth_headers=law_client.auth.get_headers(),
        )
        logging.info("Derivation created successfully id=%s", derivation.get("id"))

    finally:
        # 4. Cleanup: delete both collections
        if dataset_collection_id:
            _delete_collection(dnk_client, dataset_collection_id)
        if law_collection_id:
            _delete_collection(law_client, law_collection_id)

    logging.info("Disposable dataset-law derivation script finished")


if __name__ == "__main__":
    if config.logging_for_prod:
        logging.basicConfig(level=logging.INFO)
    else:
        logging.basicConfig(
            level=logging.INFO,
            format="%(asctime)s [%(levelname)s] %(filename)s:%(lineno)d %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S",
        )
    logging.info("=== CURRENT DATABASE: %s ===", config.database_name)
    logging.info("Executing %s...", __file__)
    main()
