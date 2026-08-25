"""
One-off script: create a disposable OGD dataset, attach csv/json/xlsx distributions,
verify they exist as expected, then delete the collection (and dataset) again.
"""

import datetime
import logging
import uuid

import config
from src.clients.dnk_client import DNKClient
from src.clients.helpers import url_join
from src.dataspot_dataset import OGDDataset

# Fake ODS id used only for label / export URL generation in this throwaway run
THROWAWAY_ODS_ID = "999999"

_DISTRIBUTION_SPECS = (
    {
        "format": "csv",
        "path_suffix": "csv/?delimiter=%3B&lang=de&timezone=Europe%2FZurich&use_labels=true",
        "description": "CSV verwendet ein Semikolon (;) als Trennzeichen.",
    },
    {
        "format": "json",
        "path_suffix": "json/?lang=de&timezone=Europe%2FZurich",
        "description": None,
    },
    {
        "format": "xlsx",
        "path_suffix": "xlsx/?lang=de&timezone=Europe%2FZurich&use_labels=true",
        "description": None,
    },
)


def _namespace() -> str:
    return (
        f"tmp-{datetime.datetime.now(datetime.timezone.utc).strftime('%Y%m%d%H%M%S')}"
        f"-{uuid.uuid4().hex[:8]}"
    )


def _create_collection(client: DNKClient, label: str) -> dict:
    endpoint = f"/rest/{config.database_name}/schemes/{config.dnk_scheme_name}/collections"
    data = {"_type": "Collection", "label": label}
    result = client._create_asset(endpoint=endpoint, data=data, status="PUBLISHED")
    logging.info(
        "Created collection id=%s label=%s", result.get("id"), label
    )
    return result


def _create_dataset(client: DNKClient, collection_id: str, namespace: str) -> dict:
    datenportal_link = f"https://data.bs.ch/explore/dataset/{THROWAWAY_ODS_ID}/"
    dataset_obj = OGDDataset(
        name=f"{namespace} OGD Distributions Test",
        datenportal_identifikation=THROWAWAY_ODS_ID,
        datenportal_link=datenportal_link,
        publish_on_i14y="yes",
        i14y_kontaktstelle_sk_id=config.ogd_i14y_kontaktstelle_sk_id,
        i14y_dataset_landing_page=datenportal_link,
    )
    dataset_payload = dataset_obj.to_json()
    dataset_payload["inCollection"] = collection_id
    endpoint = url_join(
        "rest",
        config.database_name,
        "collections",
        collection_id,
        "datasets",
        leading_slash=True,
    )
    dataset = client._create_asset(
        endpoint=endpoint, data=dataset_payload, status="PUBLISHED"
    )
    dataset_id = dataset.get("id")
    if not dataset_id:
        raise ValueError("Dataset create response missing id")
    logging.info("Created dataset id=%s odsId=%s", dataset_id, THROWAWAY_ODS_ID)
    return dataset


def _create_distributions(client: DNKClient, dataset_id: str) -> None:
    endpoint = f"/rest/{config.database_name}/datasets/{dataset_id}/distributions"
    for spec in _DISTRIBUTION_SPECS:
        fmt = spec["format"]
        access_url = (
            f"https://data.bs.ch/api/explore/v2.1/catalog/datasets/{THROWAWAY_ODS_ID}/exports/"
            f"{spec['path_suffix']}"
        )
        payload = {
            "_type": "Distribution",
            "label": f"{THROWAWAY_ODS_ID}.{fmt}",
            "accessURL": access_url,
            "format": fmt,
        }
        if spec["description"]:
            payload["description"] = spec["description"]

        result = client._create_asset(endpoint=endpoint, data=payload, status="PUBLISHED")
        logging.info(
            "Created distribution format=%s id=%s label=%s",
            fmt,
            result.get("id"),
            payload["label"],
        )


def _verify_distributions(client: DNKClient, dataset_id: str) -> None:
    endpoint = f"/rest/{config.database_name}/datasets/{dataset_id}/distributions"
    response = client._get_asset(endpoint)
    if not response or "_embedded" not in response or "distributions" not in response["_embedded"]:
        raise RuntimeError(f"No distributions returned for dataset {dataset_id}")

    distributions = response["_embedded"]["distributions"]
    by_format = {d.get("format"): d for d in distributions if d.get("format")}

    expected_formats = {spec["format"] for spec in _DISTRIBUTION_SPECS}
    missing = expected_formats - set(by_format.keys())
    if missing:
        raise RuntimeError(f"Missing distribution formats: {sorted(missing)}")

    for spec in _DISTRIBUTION_SPECS:
        fmt = spec["format"]
        dist = by_format[fmt]
        expected_label = f"{THROWAWAY_ODS_ID}.{fmt}"
        expected_url = (
            f"https://data.bs.ch/api/explore/v2.1/catalog/datasets/{THROWAWAY_ODS_ID}/exports/"
            f"{spec['path_suffix']}"
        )

        if dist.get("label") != expected_label:
            raise RuntimeError(
                f"Unexpected label for {fmt}: got {dist.get('label')!r}, "
                f"expected {expected_label!r}"
            )
        if dist.get("accessURL") != expected_url:
            raise RuntimeError(
                f"Unexpected accessURL for {fmt}: got {dist.get('accessURL')!r}, "
                f"expected {expected_url!r}"
            )
        if dist.get("status") != "PUBLISHED":
            raise RuntimeError(
                f"Unexpected status for {fmt}: got {dist.get('status')!r}, expected 'PUBLISHED'"
            )
        if spec["description"] and dist.get("description") != spec["description"]:
            raise RuntimeError(
                f"Unexpected description for {fmt}: got {dist.get('description')!r}, "
                f"expected {spec['description']!r}"
            )

    logging.info(
        "Verified %s distributions for dataset %s: %s",
        len(expected_formats),
        dataset_id,
        sorted(expected_formats),
    )


def _delete_collection(client: DNKClient, collection_id: str) -> None:
    endpoint = f"/rest/{config.database_name}/collections/{collection_id}"
    try:
        client._delete_asset(endpoint, force_delete=True)
        logging.info("Deleted collection id=%s", collection_id)
    except Exception as e:
        logging.error("Failed to delete collection id=%s: %s", collection_id, e)


def main() -> None:
    logging.info("Starting disposable OGD distributions script")
    namespace = _namespace()
    client = DNKClient()
    collection_id = None

    try:
        collection = _create_collection(client, f"{namespace}-ogd-distributions")
        collection_id = collection.get("id")
        if not collection_id:
            raise ValueError("Collection create response missing id")

        dataset = _create_dataset(client, collection_id, namespace)
        dataset_id = dataset["id"]

        _create_distributions(client, dataset_id)
        _verify_distributions(client, dataset_id)
        logging.info("OGD distributions smoke check passed")

    finally:
        if collection_id:
            _delete_collection(client, collection_id)

    logging.info("Disposable OGD distributions script finished")


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
