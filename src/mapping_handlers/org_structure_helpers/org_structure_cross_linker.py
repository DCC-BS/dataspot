import logging
from typing import Any, Dict, List, Tuple

from src.clients.base_client import BaseDataspotClient

# Maps scheme_name_short -> flat Upload API property name
CROSS_LINK_PROPERTY_BY_SCHEME_SHORT = {
    "DNK": "dnkCollectionUuid",
    "FDM": "fdmCollectionUuid",
    "RDM": "rdmCollectionUuid",
    "KV": "kvCollectionUuid",
    "SK": "skCollectionUuid",
    "TDM": "tdmCollectionUuid",
}

RowKey = Tuple[str, str | None]


def _build_sk_id_to_uuid_maps(
    clients: List[BaseDataspotClient],
) -> Dict[str, Dict[str, str]]:
    """
    Build per-scheme maps of stateCalendarId -> collection UUID.

    Returns:
        Dict keyed by scheme_name_short, each value a dict sk_id -> uuid.
    """
    maps: Dict[str, Dict[str, str]] = {}
    for client in clients:
        client.clear_collections_cache()
        units = client.get_collections_with_cache()
        sk_to_uuid: Dict[str, str] = {}
        for unit in units:
            sk_id = unit.get("stateCalendarId")
            unit_id = unit.get("id")
            if sk_id is None or not unit_id:
                continue
            sk_to_uuid[str(sk_id)] = str(unit_id)
        maps[client.scheme_name_short] = sk_to_uuid
        logging.info(
            "Cross-link map for %s: %d organisation units",
            client.scheme_name_short,
            len(sk_to_uuid),
        )
    return maps


def _counterpart_uuids(
    sk_id: str,
    sk_id_maps: Dict[str, Dict[str, str]],
) -> Dict[str, str]:
    """Flat *CollectionUuid props for one Staatskalender ID; missing twins -> \"\"."""
    desired: Dict[str, str] = {}
    for scheme_short, property_name in CROSS_LINK_PROPERTY_BY_SCHEME_SHORT.items():
        desired[property_name] = sk_id_maps.get(scheme_short, {}).get(sk_id, "")
    return desired


def _row_key(unit: Dict[str, Any]) -> RowKey | None:
    """Dedup key: (label, inCollection or None). Returns None if label is missing."""
    label = unit.get("label")
    if not label:
        return None
    in_collection = unit.get("inCollection")
    if in_collection is None or in_collection == "":
        return (str(label), None)
    return (str(label), str(in_collection))


def _build_upload_payload(
    clients: List[BaseDataspotClient],
    sk_id_maps: Dict[str, Dict[str, str]],
) -> List[Dict[str, Any]]:
    """
    Build one flat Upload-API payload shared across all schemes.

    Rows are keyed by (label, inCollection). UUID fields come from stateCalendarId.
    """
    # key -> stateCalendarId (str)
    sk_id_by_row_key: Dict[RowKey, str] = {}

    for client in clients:
        units = client.get_collections_with_cache()
        for unit in units:
            sk_id = unit.get("stateCalendarId")
            if sk_id is None:
                continue
            key = _row_key(unit)
            if key is None:
                logging.warning(
                    "Skipping unit without label in %s (id=%s)",
                    client.scheme_name_short,
                    unit.get("id"),
                )
                continue

            sk_id_str = str(sk_id)
            existing_sk_id = sk_id_by_row_key.get(key)
            if existing_sk_id is None:
                sk_id_by_row_key[key] = sk_id_str
            elif existing_sk_id != sk_id_str:
                logging.warning(
                    "Row key %s has conflicting stateCalendarId values "
                    "'%s' vs '%s'; keeping '%s'",
                    key,
                    existing_sk_id,
                    sk_id_str,
                    existing_sk_id,
                )

    payload: List[Dict[str, Any]] = []
    for (label, in_collection), sk_id_str in sorted(
        sk_id_by_row_key.items(),
        key=lambda item: (item[0][1] or "", item[0][0]),
    ):
        row: Dict[str, Any] = {
            "_type": "Collection",
            "label": label,
            **_counterpart_uuids(sk_id_str, sk_id_maps),
        }
        if in_collection is not None:
            row["inCollection"] = in_collection
        payload.append(row)

    return payload


def _upload_api_errors(response: Any) -> List[str]:
    """Extract ERROR-level messages from an Upload API response body."""
    if not isinstance(response, list):
        return []
    return [
        message.get("message", "Unknown error")
        for message in response
        if isinstance(message, dict) and message.get("level") == "ERROR"
    ]


def link_cross_scheme_uuids(clients: List[BaseDataspotClient]) -> Dict[str, int]:
    """
    Cross-link Organisationseinheit UUIDs across schemes via Upload API.

    Downloads org units (organizationalUnit + stateCalendarId) from each client
    scheme, maps stateCalendarId -> UUID per scheme, builds one flat shared
    payload, and uploads it with ADD / on_update=PUBLISHED to each scheme.

    Args:
        clients: Dataspot clients for the schemes to cross-link (DNK, FDM, RDM, KV, SK, TDM).

    Returns:
        Dict with counts: rows, schemes, missing_twin_props, uploaded, errors.
    """
    stats = {
        "rows": 0,
        "schemes": 0,
        "missing_twin_props": 0,
        "uploaded": 0,
        "errors": 0,
    }

    expected_shorts = set(CROSS_LINK_PROPERTY_BY_SCHEME_SHORT.keys())
    client_shorts = {c.scheme_name_short for c in clients}
    if client_shorts != expected_shorts:
        raise ValueError(
            f"Cross-link expects clients {sorted(expected_shorts)}, "
            f"got {sorted(client_shorts)}"
        )

    logging.info("Starting cross-scheme Organisationseinheit UUID linking...")
    sk_id_maps = _build_sk_id_to_uuid_maps(clients)
    stats["schemes"] = len(clients)

    payload = _build_upload_payload(clients, sk_id_maps)
    stats["rows"] = len(payload)

    expected_prop_count = len(CROSS_LINK_PROPERTY_BY_SCHEME_SHORT)
    for row in payload:
        filled = sum(
            1
            for prop in CROSS_LINK_PROPERTY_BY_SCHEME_SHORT.values()
            if row.get(prop)
        )
        if filled < expected_prop_count:
            stats["missing_twin_props"] += 1

    for client in clients:
        logging.info(
            "Uploading %d cross-link Collections to scheme %s (ADD, on_update=PUBLISHED)",
            len(payload),
            client.scheme_name_short,
        )
        try:
            response = client.upload_via_upload_api(
                scheme_name=client.scheme_name,
                data=payload,
                operation="ADD",
                on_update="PUBLISHED",
            )
            body_errors = _upload_api_errors(response)
            if body_errors:
                stats["errors"] += len(body_errors)
                logging.warning(
                    "Upload to scheme %s completed with %d ERROR(s)",
                    client.scheme_name_short,
                    len(body_errors),
                )
                for message in body_errors[:5]:
                    logging.error("  - %s", message)
            else:
                stats["uploaded"] += 1
                logging.info(
                    "Uploaded cross-link Collections to scheme %s",
                    client.scheme_name_short,
                )
        except Exception as e:
            stats["errors"] += 1
            logging.error(
                "Failed to upload cross-link Collections to scheme %s: %s",
                client.scheme_name_short,
                e,
            )

    logging.info(
        "Cross-scheme UUID linking finished: rows=%d schemes=%d "
        "missing_twin_props=%d uploaded=%d errors=%d",
        stats["rows"],
        stats["schemes"],
        stats["missing_twin_props"],
        stats["uploaded"],
        stats["errors"],
    )
    return stats
