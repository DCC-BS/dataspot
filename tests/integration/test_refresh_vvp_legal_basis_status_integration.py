import datetime
import logging
import os
import uuid
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional

import pytest

import config
from scripts.refresh_vvp_legal_basis_status import (
    QUALIFIER_PROPERTY,
    STATUS_CHANGED,
    STATUS_CURRENT,
    STATUS_REPEALED,
    STATUS_UNKNOWN,
    refresh_vvp_legal_basis_status,
)
from src.clients.helpers import date_to_utc_midnight_ms, url_join
from src.clients.law_client import LAWClient
from src.clients.rdm_client import RDMClient
from src.clients.vvp_client import VVPClient
from src.common import requests_delete, requests_get


pytestmark = [pytest.mark.integration]

ASSET_TYPE_ENDPOINTS = {
    "enumerations": "enumerations",
    "literals": "literals",
    "collections": "collections",
    "processings": "processings",
    "usages": "usages",
}

WRITE_STATUS = "PUBLISHED"


def _bool_env(name: str, default: bool = False) -> bool:
    value = os.getenv(name)
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "yes", "on"}


def _now_compact() -> str:
    return datetime.datetime.now(datetime.timezone.utc).strftime("%Y%m%d%H%M%S")


def _uuid8() -> str:
    return uuid.uuid4().hex[:8]


def _namespace() -> str:
    return f"IT-VVP-LBS-{_now_compact()}-{_uuid8()}"


@dataclass
class CleanupItem:
    asset_type: str
    asset_id: str


@dataclass
class CleanupManager:
    namespace: str
    auth_client: Any
    skip_cleanup: bool
    stack: List[CleanupItem] = field(default_factory=list)

    def register(self, asset_type: str, asset_id: str) -> None:
        self.stack.append(CleanupItem(asset_type=asset_type, asset_id=asset_id))

    def cleanup(self) -> None:
        if self.skip_cleanup:
            kept_ids = [f"{item.asset_type}:{item.asset_id}" for item in self.stack]
            logging.info(
                "VVP legal-basis-status integration cleanup skipped for namespace=%s retained=%s",
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
                "rest", config.database_name, endpoint_name, item.asset_id, leading_slash=True
            )
            full_url = url_join(config.base_url, endpoint)
            try:
                requests_delete(
                    full_url,
                    headers=self.auth_client.auth.get_headers(),
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
def vvp_client() -> VVPClient:
    client = VVPClient()
    logging.info("Initialized VVPClient for DB=%s", config.database_name)
    return client


@pytest.fixture(scope="session")
def rdm_client() -> RDMClient:
    client = RDMClient()
    logging.info("Initialized RDMClient for DB=%s", config.database_name)
    return client


@pytest.fixture(scope="session")
def law_collection_uuid(law_client: LAWClient) -> str:
    collection_uuid = law_client.resolve_collection_uuid_by_label(config.law_bs_collection_label)
    logging.info("Resolved LAW collection uuid=%s", collection_uuid)
    return collection_uuid


@pytest.fixture(scope="function")
def test_namespace() -> str:
    value = _namespace()
    logging.info("Created test namespace=%s", value)
    return value


@pytest.fixture(scope="function")
def cleanup_manager(test_namespace: str, vvp_client: VVPClient):
    manager = CleanupManager(
        namespace=test_namespace,
        auth_client=vvp_client,
        skip_cleanup=_bool_env("VVP_LBS_TEST_SKIP_CLEANUP", default=False),
    )
    yield manager
    manager.cleanup()


@pytest.fixture(scope="function")
def vvp_collection_uuid(
    vvp_client: VVPClient, test_namespace: str, cleanup_manager: CleanupManager
) -> str:
    collection_payload = {"_type": "Collection", "label": f"{test_namespace}-vvp-collection"}
    collection_endpoint = f"/rest/{config.database_name}/schemes/{config.vvp_scheme_name}/collections"
    collection = vvp_client._create_asset(
        endpoint=collection_endpoint, data=collection_payload, status="PUBLISHED"
    )
    collection_id = collection.get("id")
    if not collection_id:
        raise ValueError("VVP collection create response missing id")
    cleanup_manager.register("collections", collection_id)
    logging.info("Created disposable VVP collection id=%s namespace=%s", collection_id, test_namespace)
    return str(collection_id)


def create_test_law(
    law_client: LAWClient,
    collection_uuid: str,
    namespace: str,
    version_active_since_ms: Optional[int],
) -> Dict[str, Any]:
    payload = {
        "_type": "ReferenceObject",
        "label": f"{namespace} SG TEST - Test Law",
        "description": "",
        "title": namespace,
        "customProperties": {
            "systematic_number": f"9999.IT.{namespace}",
            "version_active_since": version_active_since_ms if version_active_since_ms is not None else "",
        },
    }
    law = law_client.create_reference_object(
        collection_uuid=collection_uuid, data=payload, status=WRITE_STATUS
    )
    logging.info("Created test law id=%s namespace=%s", law.get("id"), namespace)
    return law


def create_test_literal(law_client: LAWClient, parent_id: str, namespace: str) -> Dict[str, Any]:
    payload = {
        "_type": "ReferenceValue",
        "timeSeries": [
            {
                "code": f"§ {namespace}",
                "shortText": f"{namespace} literal",
                "validFrom": -2208988800000,
                "validTo": 32503593600000,
            }
        ],
    }
    literal = law_client.create_reference_value(law_id=parent_id, data=payload, status=WRITE_STATUS)
    logging.info("Created test literal id=%s namespace=%s", literal.get("id"), namespace)
    return literal


def create_test_processing(
    vvp_client: VVPClient,
    collection_uuid: str,
    namespace: str,
    current_as_of: datetime.date,
) -> Dict[str, Any]:
    payload = vvp_client.build_processing_payload(
        label=f"{namespace} Test Verfahren",
        in_collection_uuid=collection_uuid,
        legal_foundation=namespace,
        legal_foundation_source=namespace,
        website="https://example.org",
        data_processing_purpose=namespace,
        current_as_of=current_as_of,
    )
    processing = vvp_client.create_processing(
        payload=payload, in_collection_uuid=collection_uuid, status=WRITE_STATUS
    )
    logging.info("Created test processing id=%s namespace=%s", processing.get("id"), namespace)
    return processing


def create_non_law_usage_target(
    rdm_client: RDMClient,
    cleanup_manager: CleanupManager,
    namespace: str,
) -> str:
    collection_payload = {"_type": "Collection", "label": f"{namespace}-rdm-collection"}
    collection_endpoint = (
        f"/rest/{config.database_name}/schemes/{config.rdm_scheme_name}/collections"
    )
    collection = rdm_client._create_asset(
        endpoint=collection_endpoint, data=collection_payload, status="PUBLISHED"
    )
    collection_id = collection.get("id")
    if not collection_id:
        raise ValueError("RDM collection create response missing id")
    cleanup_manager.register("collections", collection_id)
    logging.info(
        "Created disposable RDM collection id=%s namespace=%s", collection_id, namespace
    )

    payload = {
        "_type": "ReferenceObject",
        "label": f"{namespace} Non-GS ReferenceObject",
        "description": "",
        "title": namespace,
    }
    endpoint = f"/rest/{config.database_name}/collections/{collection_id}/enumerations"
    reference_object = rdm_client._create_asset(
        endpoint=endpoint, data=payload, status=WRITE_STATUS
    )
    reference_object_id = reference_object.get("id")
    if not reference_object_id:
        raise ValueError("RDM ReferenceObject create response missing id")
    cleanup_manager.register("enumerations", reference_object_id)
    logging.info(
        "Created non-law usage target id=%s namespace=%s",
        reference_object_id,
        namespace,
    )
    return str(reference_object_id)


def create_test_usage(vvp_client: VVPClient, processing_id: str, usage_of_id: str) -> Dict[str, Any]:
    usage = vvp_client.create_usage(used_by_processing_uuid=processing_id, usage_of_uuid=usage_of_id)
    logging.info(
        "Created test usage id=%s processing=%s usage_of=%s",
        usage.get("id"),
        processing_id,
        usage_of_id,
    )
    return usage


def get_usage_qualifier(vvp_client: VVPClient, usage_id: str) -> Optional[str]:
    url = f"{config.base_url}/rest/{config.database_name}/usages/{usage_id}"
    response = requests_get(url=url, headers=vvp_client.auth.get_headers())
    response.raise_for_status()
    body = response.json()
    return body.get(QUALIFIER_PROPERTY)


def _make_processing_and_usage(
    vvp_client: VVPClient,
    collection_uuid: str,
    cleanup_manager: CleanupManager,
    namespace: str,
    current_as_of: datetime.date,
    usage_of_id: str,
) -> Dict[str, Any]:
    processing = create_test_processing(
        vvp_client=vvp_client,
        collection_uuid=collection_uuid,
        namespace=namespace,
        current_as_of=current_as_of,
    )
    cleanup_manager.register("processings", processing["id"])

    usage = create_test_usage(
        vvp_client=vvp_client, processing_id=processing["id"], usage_of_id=usage_of_id
    )
    cleanup_manager.register("usages", usage["id"])
    return usage


def test_usage_of_not_a_law_is_left_untouched(
    rdm_client: RDMClient,
    vvp_client: VVPClient,
    vvp_collection_uuid: str,
    cleanup_manager: CleanupManager,
    test_namespace: str,
) -> None:
    non_law_target_id = create_non_law_usage_target(
        rdm_client=rdm_client,
        cleanup_manager=cleanup_manager,
        namespace=test_namespace,
    )
    usage = _make_processing_and_usage(
        vvp_client=vvp_client,
        collection_uuid=vvp_collection_uuid,
        cleanup_manager=cleanup_manager,
        namespace=test_namespace,
        current_as_of=datetime.date(2020, 1, 1),
        usage_of_id=non_law_target_id,
    )

    refresh_vvp_legal_basis_status()

    assert get_usage_qualifier(vvp_client, usage["id"]) is None


def test_repealed_reference_object_is_ausser_kraft(
    law_client: LAWClient,
    vvp_client: VVPClient,
    law_collection_uuid: str,
    vvp_collection_uuid: str,
    cleanup_manager: CleanupManager,
    test_namespace: str,
) -> None:
    law = create_test_law(
        law_client=law_client,
        collection_uuid=law_collection_uuid,
        namespace=test_namespace,
        version_active_since_ms=date_to_utc_midnight_ms(datetime.date(2000, 1, 1)),
    )
    cleanup_manager.register("enumerations", law["id"])
    law_client.mark_reference_object_for_deletion(law["id"])

    usage = _make_processing_and_usage(
        vvp_client=vvp_client,
        collection_uuid=vvp_collection_uuid,
        cleanup_manager=cleanup_manager,
        namespace=test_namespace,
        current_as_of=datetime.date(2020, 1, 1),
        usage_of_id=law["id"],
    )

    refresh_vvp_legal_basis_status()

    assert get_usage_qualifier(vvp_client, usage["id"]) == STATUS_REPEALED


def test_repealed_reference_value_is_ausser_kraft_even_if_parent_published(
    law_client: LAWClient,
    vvp_client: VVPClient,
    law_collection_uuid: str,
    vvp_collection_uuid: str,
    cleanup_manager: CleanupManager,
    test_namespace: str,
) -> None:
    law = create_test_law(
        law_client=law_client,
        collection_uuid=law_collection_uuid,
        namespace=test_namespace,
        version_active_since_ms=date_to_utc_midnight_ms(datetime.date(2000, 1, 1)),
    )
    cleanup_manager.register("enumerations", law["id"])
    literal = create_test_literal(law_client=law_client, parent_id=law["id"], namespace=test_namespace)
    cleanup_manager.register("literals", literal["id"])
    law_client.mark_literal_for_deletion(literal["id"])

    usage = _make_processing_and_usage(
        vvp_client=vvp_client,
        collection_uuid=vvp_collection_uuid,
        cleanup_manager=cleanup_manager,
        namespace=test_namespace,
        current_as_of=datetime.date(2020, 1, 1),
        usage_of_id=literal["id"],
    )

    refresh_vvp_legal_basis_status()

    assert get_usage_qualifier(vvp_client, usage["id"]) == STATUS_REPEALED


def test_published_law_without_version_active_since_is_unbekannt(
    law_client: LAWClient,
    vvp_client: VVPClient,
    law_collection_uuid: str,
    vvp_collection_uuid: str,
    cleanup_manager: CleanupManager,
    test_namespace: str,
) -> None:
    law = create_test_law(
        law_client=law_client,
        collection_uuid=law_collection_uuid,
        namespace=test_namespace,
        version_active_since_ms=None,
    )
    cleanup_manager.register("enumerations", law["id"])

    usage = _make_processing_and_usage(
        vvp_client=vvp_client,
        collection_uuid=vvp_collection_uuid,
        cleanup_manager=cleanup_manager,
        namespace=test_namespace,
        current_as_of=datetime.date(2020, 1, 1),
        usage_of_id=law["id"],
    )

    refresh_vvp_legal_basis_status()

    assert get_usage_qualifier(vvp_client, usage["id"]) == STATUS_UNKNOWN


def test_version_active_since_after_current_as_of_is_geaendert(
    law_client: LAWClient,
    vvp_client: VVPClient,
    law_collection_uuid: str,
    vvp_collection_uuid: str,
    cleanup_manager: CleanupManager,
    test_namespace: str,
) -> None:
    law = create_test_law(
        law_client=law_client,
        collection_uuid=law_collection_uuid,
        namespace=test_namespace,
        version_active_since_ms=date_to_utc_midnight_ms(datetime.date(2030, 1, 1)),
    )
    cleanup_manager.register("enumerations", law["id"])

    usage = _make_processing_and_usage(
        vvp_client=vvp_client,
        collection_uuid=vvp_collection_uuid,
        cleanup_manager=cleanup_manager,
        namespace=test_namespace,
        current_as_of=datetime.date(2020, 1, 1),
        usage_of_id=law["id"],
    )

    refresh_vvp_legal_basis_status()

    assert get_usage_qualifier(vvp_client, usage["id"]) == STATUS_CHANGED


def test_version_active_since_before_current_as_of_is_aktuell(
    law_client: LAWClient,
    vvp_client: VVPClient,
    law_collection_uuid: str,
    vvp_collection_uuid: str,
    cleanup_manager: CleanupManager,
    test_namespace: str,
) -> None:
    law = create_test_law(
        law_client=law_client,
        collection_uuid=law_collection_uuid,
        namespace=test_namespace,
        version_active_since_ms=date_to_utc_midnight_ms(datetime.date(2010, 1, 1)),
    )
    cleanup_manager.register("enumerations", law["id"])

    usage = _make_processing_and_usage(
        vvp_client=vvp_client,
        collection_uuid=vvp_collection_uuid,
        cleanup_manager=cleanup_manager,
        namespace=test_namespace,
        current_as_of=datetime.date(2020, 1, 1),
        usage_of_id=law["id"],
    )

    refresh_vvp_legal_basis_status()

    assert get_usage_qualifier(vvp_client, usage["id"]) == STATUS_CURRENT


def test_reference_value_inherits_version_active_since_from_parent(
    law_client: LAWClient,
    vvp_client: VVPClient,
    law_collection_uuid: str,
    vvp_collection_uuid: str,
    cleanup_manager: CleanupManager,
    test_namespace: str,
) -> None:
    law = create_test_law(
        law_client=law_client,
        collection_uuid=law_collection_uuid,
        namespace=test_namespace,
        version_active_since_ms=date_to_utc_midnight_ms(datetime.date(2010, 1, 1)),
    )
    cleanup_manager.register("enumerations", law["id"])
    literal = create_test_literal(law_client=law_client, parent_id=law["id"], namespace=test_namespace)
    cleanup_manager.register("literals", literal["id"])

    usage = _make_processing_and_usage(
        vvp_client=vvp_client,
        collection_uuid=vvp_collection_uuid,
        cleanup_manager=cleanup_manager,
        namespace=test_namespace,
        current_as_of=datetime.date(2020, 1, 1),
        usage_of_id=literal["id"],
    )

    refresh_vvp_legal_basis_status()

    assert get_usage_qualifier(vvp_client, usage["id"]) == STATUS_CURRENT


def test_already_correct_status_stays_unchanged(
    law_client: LAWClient,
    vvp_client: VVPClient,
    law_collection_uuid: str,
    vvp_collection_uuid: str,
    cleanup_manager: CleanupManager,
    test_namespace: str,
) -> None:
    law = create_test_law(
        law_client=law_client,
        collection_uuid=law_collection_uuid,
        namespace=test_namespace,
        version_active_since_ms=date_to_utc_midnight_ms(datetime.date(2010, 1, 1)),
    )
    cleanup_manager.register("enumerations", law["id"])

    usage = _make_processing_and_usage(
        vvp_client=vvp_client,
        collection_uuid=vvp_collection_uuid,
        cleanup_manager=cleanup_manager,
        namespace=test_namespace,
        current_as_of=datetime.date(2020, 1, 1),
        usage_of_id=law["id"],
    )
    vvp_client.patch_usage(
        usage_uuid=usage["id"],
        payload={QUALIFIER_PROPERTY: STATUS_CURRENT},
    )

    refresh_vvp_legal_basis_status()

    assert get_usage_qualifier(vvp_client, usage["id"]) == STATUS_CURRENT
