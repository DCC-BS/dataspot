import datetime
import logging
import os
import uuid
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Set, Tuple

import pytest

import config
from scripts.sync_law_bs import (
    WRITE_STATUS,
    fetch_active_laws_from_ods,
    normalize_systematic_number,
    parse_paragraphs_from_gesetzestext_html,
    sync_law_bs,
)
from src.clients.dnk_client import DNKClient
from src.clients.law_client import LAWClient
from src.clients.helpers import url_join
from src.common import requests_delete, requests_get, requests_post
from src.dataspot_dataset import OGDDataset


pytestmark = [pytest.mark.integration]

ASSET_TYPE_ENDPOINTS = {
    "enumerations": "enumerations",
    "literals": "literals",
    "datasets": "datasets",
    "collections": "collections",
    "derivations": "derivations",
}


def _bool_env(name: str, default: bool = False) -> bool:
    value = os.getenv(name)
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "yes", "on"}


def _sql_quote(value: str) -> str:
    return value.replace("'", "''")


def _now_compact() -> str:
    return datetime.datetime.now(datetime.timezone.utc).strftime("%Y%m%d%H%M%S")


def _uuid8() -> str:
    return uuid.uuid4().hex[:8]


def _namespace() -> str:
    return f"IT-LAW-{_now_compact()}-{_uuid8()}"


def _obsolete_systematic_number(namespace: str) -> str:
    return f"9999.IT.{namespace}"


@dataclass
class CleanupItem:
    asset_type: str
    asset_id: str


@dataclass
class CleanupManager:
    namespace: str
    law_client: LAWClient
    skip_cleanup: bool
    stack: List[CleanupItem] = field(default_factory=list)

    def register(self, asset_type: str, asset_id: str) -> None:
        self.stack.append(CleanupItem(asset_type=asset_type, asset_id=asset_id))

    def cleanup(self) -> None:
        if self.skip_cleanup:
            kept_ids = [f"{item.asset_type}:{item.asset_id}" for item in self.stack]
            logging.info(
                "LAW integration cleanup skipped for namespace=%s retained=%s",
                self.namespace,
                kept_ids,
            )
            return

        for item in reversed(self.stack):
            endpoint_name = ASSET_TYPE_ENDPOINTS.get(item.asset_type)
            if not endpoint_name:
                logging.error(
                    "Unknown cleanup asset type=%s id=%s", item.asset_type, item.asset_id
                )
                continue

            endpoint = url_join(
                "rest",
                config.database_name,
                endpoint_name,
                item.asset_id,
                leading_slash=True,
            )
            full_url = url_join(config.base_url, endpoint)
            try:
                requests_delete(
                    full_url,
                    headers=self.law_client.auth.get_headers(),
                    silent_status_codes=[404, 410],
                )
                logging.info("Cleanup deleted %s id=%s", item.asset_type, item.asset_id)
            except Exception as exc:
                logging.error(
                    "Cleanup failed for %s id=%s error=%s",
                    item.asset_type,
                    item.asset_id,
                    str(exc),
                )


@pytest.fixture(scope="session")
def law_client() -> LAWClient:
    if config.database_name != config.test_database_name:
        raise AssertionError(
            "Integration tests must run against test DB only. "
            f"database_name={config.database_name}, test_database_name={config.test_database_name}"
        )
    if config.database_name == config.database_name_prod:
        raise AssertionError(
            "Integration tests cannot run against production DB. "
            f"database_name={config.database_name}, database_name_prod={config.database_name_prod}"
        )
    client = LAWClient()
    logging.info("Initialized LAWClient for DB=%s", config.database_name)
    return client


@pytest.fixture(scope="session")
def law_collection_uuid(law_client: LAWClient) -> str:
    assets = law_client.download_scheme_assets()
    collection_uuid = law_client.resolve_collection_uuid_by_label(
        assets, config.law_bs_collection_label
    )
    logging.info("Resolved LAW collection uuid=%s", collection_uuid)
    return collection_uuid


@pytest.fixture(scope="session")
def dnk_client() -> DNKClient:
    client = DNKClient()
    logging.info("Initialized DNKClient for DB=%s", config.database_name)
    return client


@pytest.fixture(scope="function")
def test_namespace() -> str:
    value = _namespace()
    logging.info("Created test namespace=%s", value)
    return value


@pytest.fixture(scope="function")
def cleanup_manager(test_namespace: str, law_client: LAWClient):
    manager = CleanupManager(
        namespace=test_namespace,
        law_client=law_client,
        skip_cleanup=_bool_env("LAW_TEST_SKIP_CLEANUP", default=False),
    )
    yield manager
    manager.cleanup()


@pytest.fixture(scope="session")
def ods_live_sample() -> Dict[str, Any]:
    sample = select_live_ods_law(require_paragraphs=False)
    logging.info(
        "Loaded ODS sample systematic_number=%s", sample.get("systematic_number", "")
    )
    return sample


def select_live_ods_law(require_paragraphs: bool = True) -> Dict[str, Any]:
    laws = fetch_active_laws_from_ods()
    for law in laws:
        systematic_number = normalize_systematic_number(law.get("systematic_number"))
        title_de = (law.get("title_de") or "").strip()
        gesetzestext_html = law.get("gesetzestext_html") or ""
        if not systematic_number or not title_de:
            continue
        if require_paragraphs and not parse_paragraphs_from_gesetzestext_html(gesetzestext_html):
            continue
        logging.info("Selected ODS law systematic_number=%s", systematic_number)
        return law
    raise AssertionError("No suitable active ODS law found")


def _download_law_assets(law_client: LAWClient) -> List[Dict[str, Any]]:
    assets = law_client.download_scheme_assets()
    logging.info("Downloaded %s scheme assets for assertions", len(assets))
    return assets


def _existing_systematic_numbers(law_client: LAWClient) -> Set[str]:
    result: Set[str] = set()
    for asset in _download_law_assets(law_client):
        if asset.get("_type") != "ReferenceObject":
            continue
        if asset.get("inCollection") != config.law_bs_collection_label:
            continue
        normalized = normalize_systematic_number(asset.get("systematic_number"))
        if normalized:
            result.add(normalized)
    logging.info("Found %s existing LAW systematic numbers", len(result))
    return result


def _existing_laws_by_systematic_number(
    law_client: LAWClient,
) -> Dict[str, Dict[str, Any]]:
    result: Dict[str, Dict[str, Any]] = {}
    for asset in _download_law_assets(law_client):
        if asset.get("_type") != "ReferenceObject":
            continue
        if asset.get("inCollection") != config.law_bs_collection_label:
            continue
        normalized = normalize_systematic_number(asset.get("systematic_number"))
        if normalized:
            result[normalized] = asset
    logging.info("Built existing LAW map with %s entries", len(result))
    return result


def select_live_ods_law_present_in_db(
    law_client: LAWClient,
) -> Tuple[Dict[str, Any], Dict[str, Any]]:
    existing_by_number = _existing_laws_by_systematic_number(law_client)
    laws = fetch_active_laws_from_ods()
    for law in laws:
        systematic_number = normalize_systematic_number(law.get("systematic_number"))
        if not systematic_number:
            continue
        if not parse_paragraphs_from_gesetzestext_html(law.get("gesetzestext_html") or ""):
            continue
        existing = existing_by_number.get(systematic_number)
        if not existing:
            continue
        return law, existing
    raise AssertionError("No ODS law present in DB with parsable paragraphs found")


def select_live_ods_law_absent_in_db(law_client: LAWClient) -> Dict[str, Any]:
    existing_numbers = _existing_systematic_numbers(law_client)
    laws = fetch_active_laws_from_ods()
    for law in laws:
        systematic_number = normalize_systematic_number(law.get("systematic_number"))
        if not systematic_number or systematic_number in existing_numbers:
            continue
        if not parse_paragraphs_from_gesetzestext_html(law.get("gesetzestext_html") or ""):
            continue
        logging.info(
            "Selected ODS law absent in DB systematic_number=%s", systematic_number
        )
        return law
    raise AssertionError(
        "No active ODS law absent from LAW DB found. This test DB may already be fully synced."
    )


def create_test_parent(
    law_client: LAWClient,
    collection_uuid: str,
    systematic_number: str,
    title: str,
    namespace: str,
) -> Dict[str, Any]:
    payload = {
        "_type": "ReferenceObject",
        "label": f"{namespace} SG {systematic_number} - {title}",
        "description": "",
        "title": namespace,
        "customProperties": {"systematic_number": systematic_number},
    }
    parent = law_client.create_reference_object(
        collection_uuid=collection_uuid,
        data=payload,
        status=WRITE_STATUS,
    )
    logging.info(
        "Created test parent id=%s systematic_number=%s",
        parent.get("id"),
        systematic_number,
    )
    return parent


def create_test_literal(
    law_client: LAWClient,
    parent_id: str,
    code: str,
    short_text: str,
) -> Dict[str, Any]:
    payload = {
        "_type": "ReferenceValue",
        "timeSeries": [
            {
                "code": code,
                "shortText": short_text,
                "validFrom": -2208988800000,
                "validTo": 32503593600000,
            }
        ],
    }
    literal = law_client.create_reference_value(
        law_id=parent_id,
        data=payload,
        status=WRITE_STATUS,
    )
    logging.info("Created test literal id=%s code=%s", literal.get("id"), code)
    return literal


def create_disposable_target_asset(
    dnk_client: DNKClient, namespace: str
) -> Dict[str, Any]:
    collection_payload = {"_type": "Collection", "label": f"{namespace}-dataset-collection"}
    collection_endpoint = f"/rest/{config.database_name}/schemes/{config.dnk_scheme_name}/collections"
    target_collection = dnk_client._create_asset(
        endpoint=collection_endpoint, data=collection_payload, status="PUBLISHED"
    )
    collection_id = target_collection.get("id")
    if not collection_id:
        raise ValueError("Dataset target collection create response missing id")

    dataset_obj = OGDDataset(
        name=f"{namespace} Test Dataset",
        datenportal_identifikation=f"tmp-{namespace}",
    )
    dataset_payload = dataset_obj.to_json()
    dataset_payload["inCollection"] = collection_id
    dataset_endpoint = url_join(
        "rest",
        config.database_name,
        "collections",
        collection_id,
        "datasets",
        leading_slash=True,
    )
    target_dataset = dnk_client._create_asset(
        endpoint=dataset_endpoint, data=dataset_payload, status="PUBLISHED"
    )
    dataset_id = target_dataset.get("id")
    if not dataset_id:
        raise ValueError("Dataset target create response missing id")

    logging.info(
        "Created disposable dataset target id=%s collection_id=%s",
        dataset_id,
        collection_id,
    )
    return {"id": dataset_id, "collection_id": collection_id}


def create_disposable_derivation(
    law_client: LAWClient,
    dataset_id: str,
    derived_from_id: str,
    qualifier: str = "LAWFUL_BASIS",
) -> Dict[str, Any]:
    url = f"{config.base_url}/rest/{config.database_name}/derivations"
    payload = {
        "_type": "Derivation",
        "derivedTo": dataset_id,
        "derivedFrom": derived_from_id,
        "qualifier": qualifier,
    }
    response = requests_post(
        url=url,
        json=payload,
        headers=law_client.auth.get_headers(),
    )
    result = response.json()
    logging.info(
        "Created derivation id=%s derivedTo=%s derivedFrom=%s qualifier=%s",
        result.get("id"),
        dataset_id,
        derived_from_id,
        qualifier,
    )
    return result


def delete_derivation(law_client: LAWClient, derivation_id: str) -> None:
    url = f"{config.base_url}/rest/{config.database_name}/derivations/{derivation_id}"
    requests_delete(
        url=url,
        headers=law_client.auth.get_headers(),
        silent_status_codes=[404, 410],
    )
    logging.info("Deleted derivation id=%s", derivation_id)


def get_asset_by_uuid(
    law_client: LAWClient, asset_type: str, asset_id: str
) -> Optional[Dict[str, Any]]:
    endpoint_name = ASSET_TYPE_ENDPOINTS.get(asset_type)
    if not endpoint_name:
        raise ValueError(f"Unknown asset_type '{asset_type}'")
    url = f"{config.base_url}/rest/{config.database_name}/{endpoint_name}/{asset_id}"
    response = requests_get(
        url=url,
        headers=law_client.auth.get_headers(),
        silent_status_codes=[404, 410],
    )
    body = response.json()
    if response.status_code in (404, 410):
        logging.info("%s id=%s not found (status=%s)", asset_type, asset_id, response.status_code)
        return None
    logging.info("Fetched %s id=%s", asset_type, asset_id)
    return body


def query_child_literals(law_client: LAWClient, parent_id: str) -> List[Dict[str, Any]]:
    sql = (
        "SELECT l.id, l.code, l.short_text, l.status "
        "FROM dataspot.literal_view l "
        f"WHERE l.literal_of = '{_sql_quote(parent_id)}'::uuid"
    )
    rows = law_client.execute_query_api(sql)
    logging.info("Queried %s child literals for parent=%s", len(rows), parent_id)
    return rows


def query_child_literals_in_use(law_client: LAWClient, parent_id: str) -> Set[str]:
    sql = (
        "SELECT DISTINCT l.id AS literal_id "
        "FROM dataspot.literal_view l "
        "JOIN dataspot.derivedfrom_view d ON d.derived_from = l.id "
        f"WHERE l.literal_of = '{_sql_quote(parent_id)}'::uuid"
    )
    rows = law_client.execute_query_api(sql)
    result = {str(row["literal_id"]) for row in rows if row.get("literal_id")}
    logging.info("Found %s in-use child literals for parent=%s", len(result), parent_id)
    return result


def query_parent_in_use(law_client: LAWClient, parent_id: str) -> bool:
    sql = (
        "SELECT DISTINCT d.derived_from "
        "FROM dataspot.derivedfrom_view d "
        f"WHERE d.derived_from = '{_sql_quote(parent_id)}'::uuid"
    )
    rows = law_client.execute_query_api(sql)
    in_use = len(rows) > 0
    logging.info("Parent in-use=%s for parent=%s", in_use, parent_id)
    return in_use


def assert_deleted(law_client: LAWClient, asset_type: str, asset_id: str) -> None:
    asset = get_asset_by_uuid(law_client, asset_type, asset_id)
    assert asset is None, f"Expected {asset_type} {asset_id} to be deleted"


def assert_status(
    law_client: LAWClient, asset_type: str, asset_id: str, expected_status: str
) -> None:
    asset = get_asset_by_uuid(law_client, asset_type, asset_id)
    assert asset is not None, f"Expected {asset_type} {asset_id} to exist"
    assert asset.get("status") == expected_status


def _ensure_not_tiny_ods_subset() -> None:
    count = len(fetch_active_laws_from_ods(max_records=15))
    assert count >= 10, (
        "Precondition failed: active ODS fetch appears tiny. "
        "Integration tests require uncapped realistic ODS set."
    )


def _create_parent_with_two_literals(
    law_client: LAWClient,
    cleanup: CleanupManager,
    collection_uuid: str,
    systematic_number: str,
    namespace: str,
) -> Tuple[Dict[str, Any], Dict[str, Any], Dict[str, Any]]:
    parent = create_test_parent(
        law_client=law_client,
        collection_uuid=collection_uuid,
        systematic_number=systematic_number,
        title="Obsolete Parent",
        namespace=namespace,
    )
    cleanup.register("enumerations", parent["id"])
    literal_a = create_test_literal(
        law_client=law_client,
        parent_id=parent["id"],
        code=f"§ {namespace}-A",
        short_text=f"{namespace} child A",
    )
    cleanup.register("literals", literal_a["id"])
    literal_b = create_test_literal(
        law_client=law_client,
        parent_id=parent["id"],
        code=f"§ {namespace}-B",
        short_text=f"{namespace} child B",
    )
    cleanup.register("literals", literal_b["id"])
    return parent, literal_a, literal_b


def test_sync_runs_once_noop(
    law_client: LAWClient,
) -> None:
    report = sync_law_bs()
    assert isinstance(report, dict)
    assert "status" in report
    assert "counts" in report


def test_case_a_obsolete_literal_not_in_use_deleted(
    law_client: LAWClient,
    law_collection_uuid: str,
    cleanup_manager: CleanupManager,
    test_namespace: str,
) -> None:
    _ensure_not_tiny_ods_subset()
    ods_law, parent = select_live_ods_law_present_in_db(law_client)
    paragraphs = parse_paragraphs_from_gesetzestext_html(ods_law["gesetzestext_html"])
    assert paragraphs, "Expected ODS law with at least one paragraph"

    obsolete = create_test_literal(
        law_client=law_client,
        parent_id=parent["id"],
        code=f"§ {test_namespace}-obsolete",
        short_text="obsolete literal",
    )
    cleanup_manager.register("literals", obsolete["id"])

    assert get_asset_by_uuid(law_client, "literals", obsolete["id"]) is not None

    report = sync_law_bs()

    assert_deleted(law_client, "literals", obsolete["id"])
    assert get_asset_by_uuid(law_client, "enumerations", parent["id"]) is not None
    assert report["counts"]["values_deleted"] >= 1
    assert report["counts"]["errors"] == 0


def test_case_b_obsolete_literal_in_use_marked(
    law_client: LAWClient,
    dnk_client: DNKClient,
    law_collection_uuid: str,
    cleanup_manager: CleanupManager,
    test_namespace: str,
) -> None:
    _ensure_not_tiny_ods_subset()
    ods_law, parent = select_live_ods_law_present_in_db(law_client)

    obsolete = create_test_literal(
        law_client=law_client,
        parent_id=parent["id"],
        code=f"§ {test_namespace}-obsolete",
        short_text="obsolete literal in use",
    )
    cleanup_manager.register("literals", obsolete["id"])

    target = create_disposable_target_asset(
        dnk_client=dnk_client,
        namespace=f"{test_namespace}-target",
    )
    cleanup_manager.register("collections", target["collection_id"])

    derivation = create_disposable_derivation(
        law_client=law_client,
        dataset_id=target["id"],
        derived_from_id=obsolete["id"],
    )
    cleanup_manager.register("derivations", derivation["id"])

    in_use_before = query_child_literals_in_use(law_client, parent["id"])
    assert obsolete["id"] in in_use_before

    report = sync_law_bs()

    assert_status(law_client, "literals", obsolete["id"], "REVIEWDCC2")
    assert get_asset_by_uuid(law_client, "enumerations", parent["id"]) is not None
    assert report["counts"]["values_marked_for_deletion"] >= 1
    assert any(
        item.get("type") == "ReferenceValue" and item.get("id") == obsolete["id"]
        for item in report.get("marked_items", [])
    )
    assert report["counts"]["errors"] == 0


def test_case_c_obsolete_parent_no_usage_deleted_with_children(
    law_client: LAWClient,
    law_collection_uuid: str,
    cleanup_manager: CleanupManager,
    test_namespace: str,
) -> None:
    _ensure_not_tiny_ods_subset()
    parent, literal_a, literal_b = _create_parent_with_two_literals(
        law_client=law_client,
        cleanup=cleanup_manager,
        collection_uuid=law_collection_uuid,
        systematic_number=_obsolete_systematic_number(test_namespace),
        namespace=test_namespace,
    )
    assert query_parent_in_use(law_client, parent["id"]) is False
    assert query_child_literals_in_use(law_client, parent["id"]) == set()

    report = sync_law_bs()

    assert_deleted(law_client, "literals", literal_a["id"])
    assert_deleted(law_client, "literals", literal_b["id"])
    assert_deleted(law_client, "enumerations", parent["id"])
    assert report["counts"]["laws_deleted"] >= 1
    assert report["counts"]["errors"] == 0


def test_case_d_obsolete_parent_directly_in_use_marked(
    law_client: LAWClient,
    dnk_client: DNKClient,
    law_collection_uuid: str,
    cleanup_manager: CleanupManager,
    test_namespace: str,
) -> None:
    _ensure_not_tiny_ods_subset()
    parent, _, _ = _create_parent_with_two_literals(
        law_client=law_client,
        cleanup=cleanup_manager,
        collection_uuid=law_collection_uuid,
        systematic_number=_obsolete_systematic_number(test_namespace),
        namespace=test_namespace,
    )

    target = create_disposable_target_asset(
        dnk_client=dnk_client,
        namespace=f"{test_namespace}-target",
    )
    cleanup_manager.register("collections", target["collection_id"])

    derivation = create_disposable_derivation(
        law_client=law_client,
        dataset_id=target["id"],
        derived_from_id=parent["id"],
    )
    cleanup_manager.register("derivations", derivation["id"])
    assert query_parent_in_use(law_client, parent["id"]) is True

    report = sync_law_bs()

    assert_status(law_client, "enumerations", parent["id"], "REVIEWDCC2")
    assert report["counts"]["laws_marked_for_deletion"] >= 1
    assert any(
        item.get("type") == "ReferenceObject" and item.get("id") == parent["id"]
        for item in report.get("marked_items", [])
    )
    assert report["counts"]["errors"] == 0


def test_case_e_obsolete_parent_child_only_in_use_marked_with_child_focus(
    law_client: LAWClient,
    dnk_client: DNKClient,
    law_collection_uuid: str,
    cleanup_manager: CleanupManager,
    test_namespace: str,
) -> None:
    _ensure_not_tiny_ods_subset()
    parent, used_literal, unused_literal = _create_parent_with_two_literals(
        law_client=law_client,
        cleanup=cleanup_manager,
        collection_uuid=law_collection_uuid,
        systematic_number=_obsolete_systematic_number(test_namespace),
        namespace=test_namespace,
    )

    target = create_disposable_target_asset(
        dnk_client=dnk_client,
        namespace=f"{test_namespace}-target",
    )
    cleanup_manager.register("collections", target["collection_id"])

    derivation = create_disposable_derivation(
        law_client=law_client,
        dataset_id=target["id"],
        derived_from_id=used_literal["id"],
    )
    cleanup_manager.register("derivations", derivation["id"])

    assert query_parent_in_use(law_client, parent["id"]) is False
    assert used_literal["id"] in query_child_literals_in_use(law_client, parent["id"])

    report = sync_law_bs()

    assert_status(law_client, "literals", used_literal["id"], "REVIEWDCC2")
    assert_deleted(law_client, "literals", unused_literal["id"])
    assert_status(law_client, "enumerations", parent["id"], "REVIEWDCC2")
    assert any(
        item.get("type") == "ReferenceValue" and item.get("id") == used_literal["id"]
        for item in report.get("marked_items", [])
    )
    assert not any(
        item.get("type") == "ReferenceObject" and item.get("id") == parent["id"]
        for item in report.get("marked_items", [])
    )
    assert report["counts"]["errors"] == 0


def test_case_f_rename_systematic_number_change_semantics(
    law_client: LAWClient,
    law_collection_uuid: str,
    cleanup_manager: CleanupManager,
    test_namespace: str,
) -> None:
    _ensure_not_tiny_ods_subset()
    ods_law = select_live_ods_law(require_paragraphs=True)
    canonical_systematic_number = normalize_systematic_number(ods_law["systematic_number"])
    old_systematic_number = _obsolete_systematic_number(f"{test_namespace}-old")

    old_parent = create_test_parent(
        law_client=law_client,
        collection_uuid=law_collection_uuid,
        systematic_number=old_systematic_number,
        title=ods_law["title_de"],
        namespace=test_namespace,
    )
    cleanup_manager.register("enumerations", old_parent["id"])
    old_literal = create_test_literal(
        law_client=law_client,
        parent_id=old_parent["id"],
        code=f"§ {test_namespace}-old",
        short_text="rename old literal",
    )
    cleanup_manager.register("literals", old_literal["id"])

    report = sync_law_bs()

    # Old object gets obsolete processing, while canonical ODS systematic number exists post-sync.
    old_parent_after = get_asset_by_uuid(law_client, "enumerations", old_parent["id"])
    assert old_parent_after is None or old_parent_after.get("status") == "REVIEWDCC2"

    existing_numbers = _existing_systematic_numbers(law_client)
    assert canonical_systematic_number in existing_numbers
    assert report["counts"]["errors"] == 0


def test_case_t6_follow_up_convergence_after_blocking_child_resolved(
    law_client: LAWClient,
    dnk_client: DNKClient,
    law_collection_uuid: str,
    cleanup_manager: CleanupManager,
    test_namespace: str,
) -> None:
    _ensure_not_tiny_ods_subset()
    parent, used_literal, _ = _create_parent_with_two_literals(
        law_client=law_client,
        cleanup=cleanup_manager,
        collection_uuid=law_collection_uuid,
        systematic_number=_obsolete_systematic_number(test_namespace),
        namespace=test_namespace,
    )

    target = create_disposable_target_asset(
        dnk_client=dnk_client,
        namespace=f"{test_namespace}-target",
    )
    cleanup_manager.register("collections", target["collection_id"])

    derivation = create_disposable_derivation(
        law_client=law_client,
        dataset_id=target["id"],
        derived_from_id=used_literal["id"],
    )
    cleanup_manager.register("derivations", derivation["id"])

    first_report = sync_law_bs()
    assert_status(law_client, "enumerations", parent["id"], "REVIEWDCC2")
    assert first_report["counts"]["errors"] == 0

    delete_derivation(law_client, derivation["id"])

    second_report = sync_law_bs()
    assert_deleted(law_client, "enumerations", parent["id"])
    assert second_report["counts"]["errors"] == 0
