"""
Template script to bluntly test all SQL queries and REST usage calls
needed by the VVP Rechtsgrundlagen autofill plan.

What this script does:
1) Executes every planned SQL query and validates basic assumptions.
2) Picks one existing Processing and existing LAW object/value candidates.
3) Creates Usage links for both object-level and value-level relations.
4) Reads usages via Query API after each mutation.
5) Updates each created usage via PATCH (without order).
6) Deletes each created usage and verifies cleanup.

This script intentionally modifies existing data during runtime and then
cleans up the created Usage records.
"""

import logging
import sys
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence, Set

# Ensure repo root is importable when running this script from scripts/templates.
PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

import config
from src.clients.vvp_client import VVPClient
from src.common import requests_delete_no_retry, requests_patch_no_retry, requests_post_no_retry


def _normalize_id(value: Any) -> str:
    if value is None:
        return ""
    return str(value).strip()


def _require(condition: bool, message: str) -> None:
    if not condition:
        raise ValueError(message)


def _sql_get_law_scheme_id() -> str:
    return f"""
        SELECT s.id
        FROM dataspot.scheme_view s
        WHERE s.label = '{config.law_scheme_name}'
          AND s.status = 'PUBLISHED'
    """


def _sql_get_law_reference_objects(law_scheme_id: str) -> str:
    return f"""
        SELECT e.id, e.label, e.description, e.in_collection
        FROM dataspot.enumeration_view e
        JOIN dataspot.collection_view c ON c.id = e.in_collection
        WHERE c.in_scheme = '{law_scheme_id}'::uuid
          AND e.status = 'PUBLISHED'
        ORDER BY e.label
    """


def _sql_get_law_reference_values(object_ids: Sequence[str]) -> str:
    object_uuid_list = ", ".join(f"'{object_id}'::uuid" for object_id in object_ids)
    return f"""
        SELECT l.id, l.literal_of, l.label, l.title, l.description
        FROM dataspot.literal_view l
        WHERE l.literal_of IN ({object_uuid_list})
          AND l.status = 'PUBLISHED'
    """


def _sql_get_processing_usages(processing_uuid: str) -> str:
    return f"""
        SELECT u.id, u.resource_id, u.usage_of, u.qualifier
        FROM dataspot.usageof_view u
        WHERE u.resource_id = '{processing_uuid}'::uuid
    """


def _sql_pick_existing_processing() -> str:
    return """
        SELECT p.id, p.label
        FROM dataspot.processing_view p
        WHERE p.status = 'PUBLISHED'
        ORDER BY p.label
        LIMIT 1
    """


def _execute_query(client: VVPClient, query: str, query_name: str) -> List[Dict[str, Any]]:
    rows = client.execute_query_api(sql_query=query)
    logging.info("Query succeeded: %s (rows=%s)", query_name, len(rows))
    return rows


def _pick_existing_processing_uuid(client: VVPClient) -> str:
    rows = _execute_query(
        client=client,
        query=_sql_pick_existing_processing(),
        query_name="pick_existing_processing",
    )
    _require(bool(rows), "No published Processing found to run Usage tests.")
    processing_uuid = _normalize_id(rows[0].get("id"))
    _require(bool(processing_uuid), "Existing Processing row has empty id.")
    logging.info("Picked existing Processing id=%s label=%s", processing_uuid, rows[0].get("label"))
    return processing_uuid


def _query_usage_rows(client: VVPClient, processing_uuid: str) -> List[Dict[str, Any]]:
    rows = _execute_query(
        client=client,
        query=_sql_get_processing_usages(processing_uuid),
        query_name="get_processing_usages",
    )
    return rows


def _usage_ids(rows: Sequence[Dict[str, Any]]) -> Set[str]:
    return {_normalize_id(row.get("id")) for row in rows if _normalize_id(row.get("id"))}


def _used_source_ids(rows: Sequence[Dict[str, Any]]) -> Set[str]:
    return {_normalize_id(row.get("usage_of")) for row in rows if _normalize_id(row.get("usage_of"))}


def _create_usage(client: VVPClient, processing_uuid: str, usage_of_uuid: str) -> Dict[str, Any]:
    url = f"{config.base_url}/rest/{config.database_name}/usages"
    payload = {
        "_type": "Usage",
        "usedBy": processing_uuid,
        "usageOf": usage_of_uuid,
    }
    response = requests_post_no_retry(
        url=url,
        json=payload,
        headers=client.auth.get_headers(),
        skip_sleep=True,
    )
    created = response.json()
    created_id = _normalize_id(created.get("id"))
    _require(bool(created_id), "Usage create response did not contain id.")
    logging.info("Usage created id=%s processing=%s usageOf=%s", created_id, processing_uuid, usage_of_uuid)
    return created


def _patch_usage(client: VVPClient, usage_uuid: str, new_usage_of_uuid: str) -> Dict[str, Any]:
    url = f"{config.base_url}/rest/{config.database_name}/usages/{usage_uuid}"
    payload = {
        "_type": "Usage",
        "usageOf": new_usage_of_uuid,
    }
    response = requests_patch_no_retry(
        url=url,
        json=payload,
        headers=client.auth.get_headers(),
        skip_sleep=True,
    )
    updated = response.json()
    logging.info("Usage updated id=%s usageOf=%s", usage_uuid, new_usage_of_uuid)
    return updated


def _delete_usage(client: VVPClient, usage_uuid: str) -> None:
    url = f"{config.base_url}/rest/{config.database_name}/usages/{usage_uuid}"
    requests_delete_no_retry(
        url=url,
        headers=client.auth.get_headers(),
        skip_sleep=True,
    )
    logging.info("Usage deleted id=%s", usage_uuid)


def _assert_usage_present(rows: Sequence[Dict[str, Any]], usage_uuid: str, expected_usage_of: str) -> None:
    match = None
    for row in rows:
        if _normalize_id(row.get("id")) == usage_uuid:
            match = row
            break
    _require(match is not None, f"Expected usage id={usage_uuid} not found in usageof_view.")
    actual_usage_of = _normalize_id(match.get("usage_of"))
    _require(
        actual_usage_of == expected_usage_of,
        f"usage_of mismatch for usage={usage_uuid}. expected={expected_usage_of} actual={actual_usage_of}",
    )
    logging.info("Usage read check passed id=%s usageOf=%s", usage_uuid, expected_usage_of)


def _assert_usage_usage_of(rows: Sequence[Dict[str, Any]], usage_uuid: str, expected_usage_of: str) -> None:
    match = None
    for row in rows:
        if _normalize_id(row.get("id")) == usage_uuid:
            match = row
            break
    _require(match is not None, f"Expected usage id={usage_uuid} for usageOf check not found.")
    actual_usage_of = _normalize_id(match.get("usage_of"))
    _require(
        actual_usage_of == expected_usage_of,
        (
            f"usage_of mismatch for usage={usage_uuid}. "
            f"expected={expected_usage_of} actual={actual_usage_of}"
        ),
    )
    logging.info("Usage patch read check passed id=%s usageOf=%s", usage_uuid, actual_usage_of)


def _assert_usage_absent(rows: Sequence[Dict[str, Any]], usage_uuid: str) -> None:
    present = any(_normalize_id(row.get("id")) == usage_uuid for row in rows)
    _require(not present, f"Usage id={usage_uuid} still present after DELETE.")
    logging.info("Usage delete read check passed id=%s", usage_uuid)


def _pick_unused_target_id(candidates: Sequence[Dict[str, Any]], used_source_ids: Set[str], label: str) -> str:
    for row in candidates:
        candidate_id = _normalize_id(row.get("id"))
        if candidate_id and candidate_id not in used_source_ids:
            logging.info("Picked %s candidate id=%s", label, candidate_id)
            return candidate_id
    raise ValueError(f"No unused {label} candidate available for Usage create test.")


def main() -> None:
    client = VVPClient()

    # SQL query 1: LAW scheme id
    scheme_rows = _execute_query(
        client=client,
        query=_sql_get_law_scheme_id(),
        query_name="get_law_scheme_id",
    )
    _require(bool(scheme_rows), "LAW scheme query returned no rows.")
    law_scheme_id = _normalize_id(scheme_rows[0].get("id"))
    _require(bool(law_scheme_id), "LAW scheme query returned empty id.")
    logging.info("LAW scheme resolved id=%s", law_scheme_id)

    # SQL query 2: LAW reference objects
    object_rows = _execute_query(
        client=client,
        query=_sql_get_law_reference_objects(law_scheme_id),
        query_name="get_law_reference_objects",
    )
    _require(bool(object_rows), "LAW object query returned no rows.")
    object_ids = [_normalize_id(row.get("id")) for row in object_rows if _normalize_id(row.get("id"))]
    _require(bool(object_ids), "LAW object query returned rows without usable ids.")
    logging.info("LAW objects resolved count=%s", len(object_ids))

    # SQL query 3: LAW reference values
    value_rows = _execute_query(
        client=client,
        query=_sql_get_law_reference_values(object_ids),
        query_name="get_law_reference_values_by_objects",
    )
    _require(bool(value_rows), "LAW value query returned no rows.")
    value_ids = [_normalize_id(row.get("id")) for row in value_rows if _normalize_id(row.get("id"))]
    _require(bool(value_ids), "LAW value query returned rows without usable ids.")
    logging.info("LAW values resolved count=%s", len(value_ids))

    processing_uuid = _pick_existing_processing_uuid(client=client)

    initial_usage_rows = _query_usage_rows(client=client, processing_uuid=processing_uuid)
    initial_usage_id_set = _usage_ids(initial_usage_rows)
    initial_source_id_set = _used_source_ids(initial_usage_rows)
    logging.info(
        "Initial processing usage snapshot loaded processing=%s usages=%s",
        processing_uuid,
        len(initial_usage_rows),
    )

    target_object_id = _pick_unused_target_id(
        candidates=object_rows,
        used_source_ids=initial_source_id_set,
        label="object",
    )
    target_object_id_patch = _pick_unused_target_id(
        candidates=object_rows,
        used_source_ids=initial_source_id_set.union({target_object_id}),
        label="object_patch",
    )
    target_value_id = _pick_unused_target_id(
        candidates=value_rows,
        used_source_ids=initial_source_id_set,
        label="value",
    )
    target_value_id_patch = _pick_unused_target_id(
        candidates=value_rows,
        used_source_ids=initial_source_id_set.union({target_value_id}),
        label="value_patch",
    )

    created_usage_ids: List[str] = []
    try:
        # REST create/read/update/delete usage for object-level link
        created_object_usage = _create_usage(
            client=client,
            processing_uuid=processing_uuid,
            usage_of_uuid=target_object_id,
        )
        created_object_usage_id = _normalize_id(created_object_usage.get("id"))
        created_usage_ids.append(created_object_usage_id)

        object_usage_rows_after_create = _query_usage_rows(client=client, processing_uuid=processing_uuid)
        _assert_usage_present(
            rows=object_usage_rows_after_create,
            usage_uuid=created_object_usage_id,
            expected_usage_of=target_object_id,
        )

        _patch_usage(
            client=client,
            usage_uuid=created_object_usage_id,
            new_usage_of_uuid=target_object_id_patch,
        )
        object_usage_rows_after_patch = _query_usage_rows(client=client, processing_uuid=processing_uuid)
        _assert_usage_usage_of(
            rows=object_usage_rows_after_patch,
            usage_uuid=created_object_usage_id,
            expected_usage_of=target_object_id_patch,
        )

        _delete_usage(client=client, usage_uuid=created_object_usage_id)
        created_usage_ids.remove(created_object_usage_id)
        object_usage_rows_after_delete = _query_usage_rows(client=client, processing_uuid=processing_uuid)
        _assert_usage_absent(rows=object_usage_rows_after_delete, usage_uuid=created_object_usage_id)

        # REST create/read/update/delete usage for value-level link
        created_value_usage = _create_usage(
            client=client,
            processing_uuid=processing_uuid,
            usage_of_uuid=target_value_id,
        )
        created_value_usage_id = _normalize_id(created_value_usage.get("id"))
        created_usage_ids.append(created_value_usage_id)

        value_usage_rows_after_create = _query_usage_rows(client=client, processing_uuid=processing_uuid)
        _assert_usage_present(
            rows=value_usage_rows_after_create,
            usage_uuid=created_value_usage_id,
            expected_usage_of=target_value_id,
        )

        _patch_usage(
            client=client,
            usage_uuid=created_value_usage_id,
            new_usage_of_uuid=target_value_id_patch,
        )
        value_usage_rows_after_patch = _query_usage_rows(client=client, processing_uuid=processing_uuid)
        _assert_usage_usage_of(
            rows=value_usage_rows_after_patch,
            usage_uuid=created_value_usage_id,
            expected_usage_of=target_value_id_patch,
        )

        _delete_usage(client=client, usage_uuid=created_value_usage_id)
        created_usage_ids.remove(created_value_usage_id)
        value_usage_rows_after_delete = _query_usage_rows(client=client, processing_uuid=processing_uuid)
        _assert_usage_absent(rows=value_usage_rows_after_delete, usage_uuid=created_value_usage_id)

        final_usage_rows = _query_usage_rows(client=client, processing_uuid=processing_uuid)
        final_usage_id_set = _usage_ids(final_usage_rows)
        _require(
            final_usage_id_set == initial_usage_id_set,
            "Final Usage IDs do not match the initial snapshot after cleanup.",
        )
        logging.info("Final usage snapshot matches initial state. Template test completed successfully.")

    finally:
        # Best-effort cleanup for partial failures.
        for usage_id in list(created_usage_ids):
            try:
                _delete_usage(client=client, usage_uuid=usage_id)
            except Exception as error:
                logging.error("Cleanup failed for usage id=%s: %s", usage_id, error)


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
