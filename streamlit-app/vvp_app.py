from difflib import SequenceMatcher
from pathlib import Path
from typing import Any, Dict, List
import sys
import time
from uuid import uuid4

import streamlit as st

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.append(str(PROJECT_ROOT))

import config
from src.clients.vvp_client import VVPClient


SUCCESS_POPUP_MESSAGE_KEY = "vvp_success_popup_message"
SUCCESS_POPUP_SHOWN_KEY = "vvp_success_popup_shown"
CREATE_ERROR_MESSAGE_KEY = "vvp_create_error_message"
EDIT_ERROR_MESSAGE_KEY = "vvp_edit_error_message"
CREATE_LABEL_KEY = "vvp_create_label"
CREATE_LEGAL_FOUNDATION_KEY = "vvp_create_legal_foundation"
CREATE_LEGAL_FOUNDATION_SOURCE_KEY = "vvp_create_legal_foundation_source"
CREATE_WEBSITE_KEY = "vvp_create_website"
CREATE_PURPOSE_KEY = "vvp_create_data_processing_purpose"
CREATE_FORM_VERSION_KEY = "vvp_create_form_version"
EDIT_PROCESSING_VERSION_KEY = "vvp_edit_processing_version"
LAW_SCHEME_ID_KEY = "vvp_law_scheme_id"
LAW_REFERENCE_OBJECTS_KEY = "vvp_law_reference_objects"
LAW_REFERENCE_VALUES_CACHE_KEY = "vvp_law_reference_values_cache"
CREATE_LAW_ROWS_KEY = "vvp_create_law_rows"
CREATE_PREV_AUTO_URLS_KEY = "vvp_create_prev_auto_urls"
EDIT_LAW_ROWS_KEY = "vvp_edit_law_rows"
EDIT_PREV_AUTO_URLS_KEY = "vvp_edit_prev_auto_urls"
EDIT_LAW_ROWS_FOR_PROCESSING_KEY = "vvp_edit_law_rows_for_processing"
EDIT_INITIAL_USAGE_TARGETS_KEY = "vvp_edit_initial_usage_targets"


def set_success_popup(message: str) -> None:
    st.session_state[SUCCESS_POPUP_MESSAGE_KEY] = message
    st.session_state[SUCCESS_POPUP_SHOWN_KEY] = False


def render_success_popup_once() -> None:
    message = st.session_state.get(SUCCESS_POPUP_MESSAGE_KEY, "")
    if not message:
        return

    if not st.session_state.get(SUCCESS_POPUP_SHOWN_KEY, False):
        st.toast(message, icon="✅")
        st.session_state[SUCCESS_POPUP_SHOWN_KEY] = True
        time.sleep(1.0)
        st.session_state.pop(SUCCESS_POPUP_MESSAGE_KEY, None)
        st.session_state.pop(SUCCESS_POPUP_SHOWN_KEY, None)


def normalize_text(value: Any) -> str:
    if value is None:
        return ""
    return str(value).strip().casefold()


def extract_leaf_label(value: Any) -> str:
    raw = str(value or "").strip()
    if "/" in raw:
        return raw.split("/")[-1].strip()
    return raw


def normalize_url_key(value: Any) -> str:
    return str(value or "").strip().casefold()


def split_source_lines(source_text: str) -> List[str]:
    lines: List[str] = []
    for line in str(source_text or "").splitlines():
        stripped = line.strip()
        if stripped:
            lines.append(stripped)
    return lines


def dedupe_urls(urls: List[str]) -> List[str]:
    deduped: List[str] = []
    seen: set[str] = set()
    for url in urls:
        stripped = str(url or "").strip()
        if not stripped:
            continue
        key = normalize_url_key(stripped)
        if key in seen:
            continue
        seen.add(key)
        deduped.append(stripped)
    return deduped


def make_empty_law_row() -> Dict[str, Any]:
    return {
        "row_id": str(uuid4()),
        "object_id": "",
        "value_ids": [],
    }


def ensure_trailing_empty_row(rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    normalized_rows = []
    for row in rows:
        row_id = str(row.get("row_id") or uuid4())
        object_id = str(row.get("object_id") or "").strip()
        value_ids = [str(item).strip() for item in row.get("value_ids", []) if str(item).strip()]
        normalized_rows.append({"row_id": row_id, "object_id": object_id, "value_ids": value_ids})
    if not normalized_rows:
        normalized_rows.append(make_empty_law_row())
        return normalized_rows
    last_row = normalized_rows[-1]
    if last_row["object_id"] or last_row["value_ids"]:
        normalized_rows.append(make_empty_law_row())
    return normalized_rows


def build_row_urls(
    row: Dict[str, Any],
    object_lookup: Dict[str, Dict[str, Any]],
    value_lookup_by_object: Dict[str, Dict[str, Dict[str, Any]]],
) -> List[str]:
    urls: List[str] = []
    object_id = str(row.get("object_id") or "").strip()
    if object_id:
        object_url = str(object_lookup.get(object_id, {}).get("source_url") or "").strip()
        if object_url:
            urls.append(object_url)

    object_values = value_lookup_by_object.get(object_id, {})
    for value_id in row.get("value_ids", []):
        value_url = str(object_values.get(str(value_id).strip(), {}).get("source_url") or "").strip()
        if value_url:
            urls.append(value_url)
    return urls


def collect_selected_law_target_ids(rows: List[Dict[str, Any]]) -> List[str]:
    selected_ids: List[str] = []
    seen: set[str] = set()
    for row in rows:
        value_ids = [str(item).strip() for item in row.get("value_ids", []) if str(item).strip()]
        if value_ids:
            for value_id in value_ids:
                if value_id in seen:
                    continue
                seen.add(value_id)
                selected_ids.append(value_id)
            continue
        object_id = str(row.get("object_id") or "").strip()
        if not object_id or object_id in seen:
            continue
        seen.add(object_id)
        selected_ids.append(object_id)
    return selected_ids


def sort_value_ids_by_option_order(value_ids: List[str], value_options: List[Dict[str, Any]]) -> List[str]:
    option_order = {
        str(option.get("id")): index
        for index, option in enumerate(value_options)
        if str(option.get("id", "")).strip()
    }
    deduped_ids: List[str] = []
    seen_ids: set[str] = set()
    for value_id in value_ids:
        normalized_value_id = str(value_id or "").strip()
        if not normalized_value_id or normalized_value_id in seen_ids:
            continue
        seen_ids.add(normalized_value_id)
        deduped_ids.append(normalized_value_id)
    return sorted(deduped_ids, key=lambda value_id: (option_order.get(value_id, len(option_order)), value_id.casefold()))


def rank_options_by_search(options: List[Dict[str, Any]], query: str) -> List[Dict[str, Any]]:
    sorted_options = sorted(options, key=lambda item: str(item["label"]).casefold())
    normalized_query = normalize_text(query)
    if not normalized_query:
        return sorted_options

    ranked: List[Dict[str, Any]] = []
    for option in sorted_options:
        label = str(option["label"])
        search_label = str(option.get("search_label", label))
        normalized_label = normalize_text(search_label)
        if normalized_label == normalized_query:
            rank_key = (0, 0, -1.0, label.casefold())
        elif normalized_label.startswith(normalized_query):
            rank_key = (1, 0, -1.0, label.casefold())
        else:
            similarity = SequenceMatcher(None, normalized_query, normalized_label).ratio()
            rank_key = (2, 0, -similarity, label.casefold())
        ranked.append({"option": option, "rank_key": rank_key})

    ranked_sorted = sorted(ranked, key=lambda item: item["rank_key"])
    return [item["option"] for item in ranked_sorted]


def searchable_dropdown(
    title: str,
    options: List[Dict[str, Any]],
    widget_prefix: str,
    selected_id: str = "",
) -> Dict[str, Any] | None:
    search_query = st.text_input(f"Suche: {title}", key=f"{widget_prefix}_search")
    ranked_options = rank_options_by_search(options, search_query)
    if not ranked_options:
        st.warning(f"Keine Treffer fuer {title}.")
        return None

    labels = [str(option["label"]) for option in ranked_options]
    default_index = 0
    if selected_id:
        for index, option in enumerate(ranked_options):
            if str(option.get("id")) == str(selected_id):
                default_index = index
                break

    selected_label = st.selectbox(title, options=labels, index=default_index, key=f"{widget_prefix}_select")
    for option in ranked_options:
        if str(option["label"]) == selected_label:
            return option
    return None


def searchable_combobox_no_default(
    title: str,
    options: List[Dict[str, Any]],
    widget_prefix: str,
    selected_id: str = "",
) -> Dict[str, Any] | None:
    if not options:
        st.warning(f"Keine Optionen fuer {title} verfuegbar.")
        return None

    labels = [str(option["label"]) for option in options]
    default_index = None
    if selected_id:
        for index, option in enumerate(options):
            if str(option.get("id")) == str(selected_id):
                default_index = index
                break

    selected_label = st.selectbox(
        title,
        options=labels,
        index=default_index,
        key=f"{widget_prefix}_combo",
        placeholder=f"Tippen zum Suchen ({title})",
    )
    if selected_label is None:
        return None

    for option in options:
        if str(option["label"]) == selected_label:
            return option
    return None


@st.cache_resource
def get_vvp_client() -> VVPClient:
    return VVPClient()


def get_departements_cached(client: VVPClient) -> List[Dict[str, Any]]:
    if "vvp_departements" not in st.session_state:
        with st.spinner("Lade Departemente..."):
            st.session_state["vvp_departements"] = client.get_departements()
    return st.session_state["vvp_departements"]


def get_abteilungen_cached(client: VVPClient, departement_id: str) -> List[Dict[str, Any]]:
    cached_dep_id = st.session_state.get("vvp_abteilungen_for_departement_id")
    if cached_dep_id != departement_id:
        with st.spinner("Lade Abteilungen..."):
            st.session_state["vvp_abteilungen"] = client.get_abteilungen(departement_id)
            st.session_state["vvp_abteilungen_for_departement_id"] = departement_id
    return st.session_state.get("vvp_abteilungen", [])


def get_collection_context_cached(client: VVPClient, abteilung_id: str) -> Dict[str, Any]:
    cached_abt_id = st.session_state.get("vvp_context_for_abteilung_id")
    if cached_abt_id != abteilung_id:
        with st.spinner("Lade Verfahren mit Personendaten..."):
            st.session_state["vvp_collection_context"] = client.get_collection_tree_context(abteilung_id)
            st.session_state["vvp_context_for_abteilung_id"] = abteilung_id
    return st.session_state.get("vvp_collection_context", {})


def get_law_context_cached(client: VVPClient) -> Dict[str, Any]:
    if LAW_SCHEME_ID_KEY not in st.session_state:
        with st.spinner("Lade Rechtsgrundlagen (Schema)..."):
            st.session_state[LAW_SCHEME_ID_KEY] = client.law_client.get_scheme_id()
    if LAW_REFERENCE_OBJECTS_KEY not in st.session_state:
        with st.spinner("Lade Rechtsgrundlagen (Erlasse)..."):
            st.session_state[LAW_REFERENCE_OBJECTS_KEY] = client.get_law_reference_objects()
    if LAW_REFERENCE_VALUES_CACHE_KEY not in st.session_state:
        st.session_state[LAW_REFERENCE_VALUES_CACHE_KEY] = {}
    return {
        "law_scheme_id": st.session_state[LAW_SCHEME_ID_KEY],
        "objects": st.session_state[LAW_REFERENCE_OBJECTS_KEY],
        "values_cache": st.session_state[LAW_REFERENCE_VALUES_CACHE_KEY],
    }


def get_law_values_for_object_cached(client: VVPClient, object_id: str) -> List[Dict[str, Any]]:
    normalized_object_id = str(object_id or "").strip()
    if not normalized_object_id:
        return []
    values_cache = st.session_state.setdefault(LAW_REFERENCE_VALUES_CACHE_KEY, {})
    if normalized_object_id not in values_cache:
        values_cache[normalized_object_id] = client.get_law_reference_values_by_object(normalized_object_id)
    return values_cache[normalized_object_id]


def clear_dependent_caches() -> None:
    for key in [
        "vvp_abteilungen",
        "vvp_abteilungen_for_departement_id",
        "vvp_collection_context",
        "vvp_context_for_abteilung_id",
    ]:
        if key in st.session_state:
            del st.session_state[key]


def build_law_rows_from_resolved_assets(
    usage_target_ids: List[str],
    resolved_asset_lookup: Dict[str, Dict[str, Any]],
    object_lookup: Dict[str, Dict[str, Any]],
) -> List[Dict[str, Any]]:
    grouped_rows_by_object_id: Dict[str, Dict[str, Any]] = {}
    ordered_object_ids: List[str] = []

    for usage_target_id in usage_target_ids:
        normalized_usage_target_id = str(usage_target_id or "").strip()
        if not normalized_usage_target_id:
            continue
        asset = resolved_asset_lookup.get(normalized_usage_target_id, {})
        asset_type = str(asset.get("_type") or "").strip()

        if asset_type == "ReferenceObject":
            object_id = normalized_usage_target_id
            value_id = ""
        elif asset_type == "ReferenceValue":
            object_id = str(asset.get("literal_of") or "").strip()
            value_id = normalized_usage_target_id
        else:
            continue

        if not object_id or object_id not in object_lookup:
            continue

        if object_id not in grouped_rows_by_object_id:
            grouped_rows_by_object_id[object_id] = {
                "row_id": str(uuid4()),
                "object_id": object_id,
                "value_ids": [],
            }
            ordered_object_ids.append(object_id)
        if value_id:
            row_value_ids = grouped_rows_by_object_id[object_id]["value_ids"]
            if value_id not in row_value_ids:
                row_value_ids.append(value_id)

    rows = [grouped_rows_by_object_id[object_id] for object_id in ordered_object_ids]
    return ensure_trailing_empty_row(rows)


def sync_source_field_with_selected_urls(
    *,
    source_state_key: str,
    previous_auto_urls_state_key: str,
    selected_rows: List[Dict[str, Any]],
    object_lookup: Dict[str, Dict[str, Any]],
    value_lookup_by_object: Dict[str, Dict[str, Dict[str, Any]]],
) -> None:
    selected_url_lists = [
        build_row_urls(
            row=row,
            object_lookup=object_lookup,
            value_lookup_by_object=value_lookup_by_object,
        )
        for row in selected_rows
    ]
    current_auto_urls = dedupe_urls([url for urls in selected_url_lists for url in urls])
    current_auto_keys = {normalize_url_key(url) for url in current_auto_urls}

    previous_auto_urls = st.session_state.get(previous_auto_urls_state_key, [])
    previous_auto_keys = {normalize_url_key(url) for url in previous_auto_urls}
    if current_auto_keys == previous_auto_keys:
        return

    existing_lines = split_source_lines(str(st.session_state.get(source_state_key, "")))
    existing_map: Dict[str, str] = {}
    existing_order: List[str] = []
    for line in existing_lines:
        key = normalize_url_key(line)
        if key in existing_map:
            continue
        existing_map[key] = line
        existing_order.append(key)

    keys_to_remove = previous_auto_keys - current_auto_keys
    if keys_to_remove:
        existing_order = [key for key in existing_order if key not in keys_to_remove]
        for key in keys_to_remove:
            existing_map.pop(key, None)

    for auto_url in current_auto_urls:
        key = normalize_url_key(auto_url)
        if key in existing_map:
            continue
        existing_map[key] = auto_url
        existing_order.append(key)

    merged_lines = [existing_map[key] for key in existing_order]
    st.session_state[source_state_key] = "\n".join(merged_lines)
    st.session_state[previous_auto_urls_state_key] = current_auto_urls


def render_legal_basis_rows(
    *,
    client: VVPClient,
    rows_state_key: str,
    widget_prefix: str,
    object_options: List[Dict[str, Any]],
) -> List[Dict[str, Any]]:
    rows = ensure_trailing_empty_row(st.session_state.get(rows_state_key, [make_empty_law_row()]))
    st.session_state[rows_state_key] = rows

    object_lookup = {str(option.get("id")): option for option in object_options}
    all_selected_object_ids = [str(row.get("object_id") or "").strip() for row in rows if str(row.get("object_id") or "").strip()]
    selected_value_ids_by_row: Dict[str, List[str]] = {
        str(row.get("row_id")): [str(value_id).strip() for value_id in row.get("value_ids", []) if str(value_id).strip()]
        for row in rows
    }
    remove_row_ids: set[str] = set()

    for index, row in enumerate(rows):
        row_id = str(row.get("row_id"))
        current_object_id = str(row.get("object_id") or "").strip()
        other_selected_object_ids = {
            object_id
            for object_id in all_selected_object_ids
            if object_id and object_id != current_object_id
        }
        object_options_for_row = [
            option
            for option in object_options
            if str(option.get("id")) == current_object_id or str(option.get("id")) not in other_selected_object_ids
        ]

        col_object, col_values, col_remove = st.columns([4, 4, 1])
        with col_object:
            selected_object = searchable_combobox_no_default(
                title=f"Rechtsgrundlage {index + 1}",
                options=object_options_for_row,
                widget_prefix=f"{widget_prefix}_{row_id}_object",
                selected_id=current_object_id,
            )
            new_object_id = str(selected_object.get("id") if selected_object else "").strip()
            if new_object_id != current_object_id:
                row["object_id"] = new_object_id
                row["value_ids"] = []
            elif not new_object_id:
                row["object_id"] = ""
                row["value_ids"] = []

        value_options = get_law_values_for_object_cached(client=client, object_id=row.get("object_id", ""))
        value_lookup = {str(value.get("id")): value for value in value_options}
        other_selected_value_ids: set[str] = set()
        for other_row_id, selected_ids in selected_value_ids_by_row.items():
            if other_row_id == row_id:
                continue
            other_selected_value_ids.update(selected_ids)
        value_options_for_row = [
            value
            for value in value_options
            if str(value.get("id")) in row.get("value_ids", []) or str(value.get("id")) not in other_selected_value_ids
        ]
        row["value_ids"] = sort_value_ids_by_option_order(
            [str(value_id).strip() for value_id in row.get("value_ids", []) if str(value_id).strip()],
            value_options_for_row,
        )
        value_labels = [str(value.get("label", "")).strip() for value in value_options_for_row if str(value.get("label", "")).strip()]
        value_label_to_id = {
            str(value.get("label", "")).strip(): str(value.get("id"))
            for value in value_options_for_row
            if str(value.get("label", "")).strip()
        }
        selected_value_labels = [
            str(value_lookup.get(str(value_id), {}).get("label", "")).strip()
            for value_id in row.get("value_ids", [])
            if str(value_lookup.get(str(value_id), {}).get("label", "")).strip()
        ]

        with col_values:
            selected_value_labels = st.multiselect(
                "Rechtsnormen (optional)",
                options=value_labels,
                default=selected_value_labels,
                key=f"{widget_prefix}_{row_id}_values",
                placeholder="Rechtsnorm wählen",
            )
            row["value_ids"] = sort_value_ids_by_option_order(
                [value_label_to_id[label] for label in selected_value_labels if label in value_label_to_id],
                value_options_for_row,
            )

        with col_remove:
            st.write("")
            st.write("")
            if st.button("X", key=f"{widget_prefix}_{row_id}_remove", help="Zeile entfernen"):
                remove_row_ids.add(row_id)

    if remove_row_ids:
        rows = [row for row in rows if str(row.get("row_id")) not in remove_row_ids]
    row_count_before_finalize = len(rows)
    rows = ensure_trailing_empty_row(rows)
    st.session_state[rows_state_key] = rows
    if len(rows) != row_count_before_finalize:
        st.rerun()
    return rows


def build_collection_options(collections: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    options: List[Dict[str, Any]] = []
    for collection in collections:
        collection_id = str(collection.get("id", "")).strip()
        label = str(collection.get("label", "")).strip()
        if not collection_id or not label:
            continue
        options.append(
            {
                "id": collection_id,
                "label": label,
                "search_label": extract_leaf_label(label),
            }
        )
    return sorted(options, key=lambda item: item["label"].casefold())


def build_law_object_options(objects: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    options: List[Dict[str, Any]] = []
    for obj in objects:
        object_id = str(obj.get("id", "")).strip()
        label = str(obj.get("label", "")).strip()
        if not object_id or not label:
            continue
        options.append(
            {
                "id": object_id,
                "label": label,
                "search_label": extract_leaf_label(label),
                "source_url": str(obj.get("source_url", "")).strip(),
                "description": str(obj.get("description", "")).strip(),
            }
        )
    return sorted(options, key=lambda item: item["label"].casefold())


def render_processing_list(client: VVPClient, processings: List[Dict[str, Any]], collection_lookup: Dict[str, Dict[str, Any]]) -> None:
    st.subheader("Vorhandene Verfahren mit Personendaten")
    if not processings:
        st.info("In der gewählten Abteilung wurden keine Verfahren gefunden.")
        return

    display_rows = [
        client.map_download_processing_to_display(processing, collection_lookup)
        for processing in processings
    ]
    responsible_options = sorted(
        {
            str(row.get("verantwortliche_stelle", "")).strip()
            for row in display_rows
            if str(row.get("verantwortliche_stelle", "")).strip()
        }
    )
    selected_responsible = st.selectbox(
        "Verantwortliche Stelle (optional)",
        options=responsible_options,
        index=None,
        placeholder="Keine Filterung",
        key="processing_responsible_filter",
    )

    filtered_rows = display_rows
    if selected_responsible:
        filtered_rows = [
            row for row in display_rows
            if str(row.get("verantwortliche_stelle", "")).strip() == selected_responsible
        ]

    sorted_rows = sorted(
        filtered_rows,
        key=lambda row: (
            str(row.get("verantwortliche_stelle", "")).casefold(),
            str(row.get("bezeichnung", "")).casefold(),
        ),
    )

    table_rows = [
        {
            "Bezeichnung": row.get("bezeichnung", ""),
            "Verantwortliche Stelle": row.get("verantwortliche_stelle", ""),
        }
        for row in sorted_rows
    ]
    st.dataframe(table_rows, width="stretch")


def render_edit_form(
    client: VVPClient,
    processings: List[Dict[str, Any]],
    collection_options: List[Dict[str, Any]],
) -> None:
    with st.expander("Bestehendes Verfahren bearbeiten", expanded=False):
        st.markdown(
            """
            <style>
            button[kind="primaryFormSubmit"] {
                background-color: #b42318 !important;
                border-color: #b42318 !important;
                color: #ffffff !important;
            }
            button[kind="primaryFormSubmit"]:hover {
                background-color: #912018 !important;
                border-color: #912018 !important;
            }
            </style>
            """,
            unsafe_allow_html=True,
        )
        edit_error = str(st.session_state.get(EDIT_ERROR_MESSAGE_KEY, "")).strip()
        if edit_error:
            st.error(edit_error)

        if not processings:
            st.info("Es gibt aktuell kein Verfahren zum Bearbeiten.")
            return

        processing_options = sorted(
            [{"id": str(item.get("id", "")), "label": str(item.get("label", "")).strip()} for item in processings if item.get("id") and item.get("label")],
            key=lambda item: item["label"].casefold(),
        )
        edit_processing_version = int(st.session_state.get(EDIT_PROCESSING_VERSION_KEY, 0))
        edit_processing_prefix = f"edit_processing_{edit_processing_version}"
        selected_processing = searchable_combobox_no_default(
            title="Verfahren auswählen",
            options=processing_options,
            widget_prefix=edit_processing_prefix,
        )
        if not selected_processing:
            return
        selected_processing_id = selected_processing["id"]

        rest_processing = client.get_processing_by_uuid(selected_processing_id)
        form_values = client.map_rest_processing_to_form(rest_processing)
        law_context = get_law_context_cached(client)
        law_scheme_id = str(law_context["law_scheme_id"]).strip()
        law_object_options = build_law_object_options(law_context["objects"])
        law_object_lookup = {str(item.get("id")): item for item in law_object_options}

        edit_rows_for_processing = str(st.session_state.get(EDIT_LAW_ROWS_FOR_PROCESSING_KEY, "")).strip()
        if edit_rows_for_processing != selected_processing_id:
            # TODO(dataspot-4129): The Query API currently returns NULL literal labels (`literal_view.label`),
            # which breaks single-query prefill for ReferenceValues. Once https://issues.dataspot.io/issues/4129
            # is fixed, replace this multi-call REST resolution with the query below:
            #
            # WITH selected_processing AS (
            #   SELECT p.id, p.model_id
            #   FROM dataspot.processing_view p
            #   WHERE p.id = '<processing_uuid>'::uuid
            #     AND p.status = 'PUBLISHED'
            # )
            # SELECT
            #   u.id AS usage_id,
            #   u.usage_of,
            #   u.model_id AS usage_model_id,
            #   obj.id AS object_id,
            #   obj.label AS object_label,
            #   val.id AS value_id,
            #   val.label AS value_label,
            #   val.literal_of AS value_object_id,
            #   parent_obj.label AS value_object_label
            # FROM dataspot.usageof_view u
            # JOIN selected_processing p
            #   ON p.id = u.resource_id
            #  AND p.model_id = u.model_id
            # LEFT JOIN dataspot.enumeration_view obj
            #   ON obj.id = u.usage_of
            #  AND obj.status = 'PUBLISHED'
            # LEFT JOIN dataspot.literal_view val
            #   ON val.id = u.usage_of
            #  AND val.status = 'PUBLISHED'
            # LEFT JOIN dataspot.enumeration_view parent_obj
            #   ON parent_obj.id = val.literal_of
            #  AND parent_obj.status = 'PUBLISHED'
            # ORDER BY u.id;
            usage_rows = client.get_processing_usage_targets(processing_uuid=selected_processing_id)
            raw_usage_target_ids = [
                str(usage_row.get("usage_of") or "").strip()
                for usage_row in usage_rows
                if str(usage_row.get("usage_of") or "").strip()
            ]
            unique_usage_target_ids: List[str] = []
            seen_usage_target_ids: set[str] = set()
            for usage_target_id in raw_usage_target_ids:
                if usage_target_id in seen_usage_target_ids:
                    continue
                seen_usage_target_ids.add(usage_target_id)
                unique_usage_target_ids.append(usage_target_id)

            resolved_asset_lookup: Dict[str, Dict[str, Any]] = {}
            for usage_target_id in unique_usage_target_ids:
                resolved_asset_lookup[usage_target_id] = client.get_asset_by_uuid(usage_target_id)

            filtered_usage_target_ids: List[str] = []
            required_object_ids: set[str] = set()
            for usage_target_id in unique_usage_target_ids:
                resolved_asset = resolved_asset_lookup.get(usage_target_id, {})
                asset_type = str(resolved_asset.get("_type") or "").strip()
                asset_status = str(resolved_asset.get("status") or "").strip()
                asset_model_id = str(resolved_asset.get("model_id") or "").strip()
                if asset_model_id != law_scheme_id or asset_status != "PUBLISHED":
                    continue
                if asset_type == "ReferenceObject":
                    filtered_usage_target_ids.append(usage_target_id)
                    required_object_ids.add(usage_target_id)
                    continue
                if asset_type != "ReferenceValue":
                    continue
                parent_object_id = str(resolved_asset.get("literal_of") or "").strip()
                if not parent_object_id:
                    continue
                filtered_usage_target_ids.append(usage_target_id)
                required_object_ids.add(parent_object_id)

            # Minimal fallback: only resolve missing preselected object IDs that are required for rendering.
            missing_object_ids = [object_id for object_id in required_object_ids if object_id not in law_object_lookup]
            for missing_object_id in missing_object_ids:
                fallback_object_asset = client.get_asset_by_uuid(missing_object_id)
                fallback_type = str(fallback_object_asset.get("_type") or "").strip()
                fallback_status = str(fallback_object_asset.get("status") or "").strip()
                fallback_model_id = str(fallback_object_asset.get("model_id") or "").strip()
                fallback_label = str(fallback_object_asset.get("label") or "").strip()
                if (
                    fallback_type != "ReferenceObject"
                    or fallback_status != "PUBLISHED"
                    or fallback_model_id != law_scheme_id
                    or not fallback_label
                ):
                    continue
                fallback_option = {
                    "id": missing_object_id,
                    "label": fallback_label,
                    "search_label": extract_leaf_label(fallback_label),
                    "source_url": str(fallback_object_asset.get("source_url") or "").strip(),
                    "description": str(fallback_object_asset.get("description") or "").strip(),
                }
                law_object_options.append(fallback_option)
                law_object_lookup[missing_object_id] = fallback_option
            law_object_options = sorted(law_object_options, key=lambda item: str(item.get("label", "")).casefold())

            st.session_state[EDIT_LAW_ROWS_KEY] = build_law_rows_from_resolved_assets(
                usage_target_ids=filtered_usage_target_ids,
                resolved_asset_lookup=resolved_asset_lookup,
                object_lookup=law_object_lookup,
            )
            st.session_state[EDIT_LAW_ROWS_FOR_PROCESSING_KEY] = selected_processing_id
            st.session_state[EDIT_INITIAL_USAGE_TARGETS_KEY] = filtered_usage_target_ids
            st.session_state[EDIT_PREV_AUTO_URLS_KEY] = []

        source_state_key = f"vvp_edit_source_text_{selected_processing_id}"
        if source_state_key not in st.session_state:
            st.session_state[source_state_key] = form_values["legalFoundationSource"]
        if f"vvp_edit_legal_foundation_{selected_processing_id}" not in st.session_state:
            st.session_state[f"vvp_edit_legal_foundation_{selected_processing_id}"] = form_values["legalFoundation"]
        if f"vvp_edit_label_{selected_processing_id}" not in st.session_state:
            st.session_state[f"vvp_edit_label_{selected_processing_id}"] = form_values["label"]
        if f"vvp_edit_website_{selected_processing_id}" not in st.session_state:
            st.session_state[f"vvp_edit_website_{selected_processing_id}"] = form_values["website"]
        if f"vvp_edit_purpose_{selected_processing_id}" not in st.session_state:
            st.session_state[f"vvp_edit_purpose_{selected_processing_id}"] = form_values["dataProcessingPurpose"]

        selected_collection = searchable_combobox_no_default(
            title="Verantwortliche Stelle",
            options=collection_options,
            widget_prefix="edit_collection",
            selected_id=form_values["inCollection"],
        )
        label = st.text_input("Bezeichnung", key=f"vvp_edit_label_{selected_processing_id}")
        legal_foundation = st.text_area("Rechtliche Grundlage(n)", key=f"vvp_edit_legal_foundation_{selected_processing_id}")

        rows = render_legal_basis_rows(
            client=client,
            rows_state_key=EDIT_LAW_ROWS_KEY,
            widget_prefix=f"edit_law_{selected_processing_id}",
            object_options=law_object_options,
        )
        value_lookup_by_object: Dict[str, Dict[str, Dict[str, Any]]] = {}
        for row in rows:
            object_id = str(row.get("object_id") or "").strip()
            if not object_id or object_id in value_lookup_by_object:
                continue
            object_values = get_law_values_for_object_cached(client=client, object_id=object_id)
            value_lookup_by_object[object_id] = {str(item.get("id")): item for item in object_values}
        sync_source_field_with_selected_urls(
            source_state_key=source_state_key,
            previous_auto_urls_state_key=EDIT_PREV_AUTO_URLS_KEY,
            selected_rows=rows,
            object_lookup=law_object_lookup,
            value_lookup_by_object=value_lookup_by_object,
        )

        legal_foundation_source = st.text_area("Quelle(n)", key=source_state_key)
        website = st.text_input("Internetauftritt", key=f"vvp_edit_website_{selected_processing_id}")
        data_processing_purpose = st.text_area("Zweck der Datenbearbeitung", key=f"vvp_edit_purpose_{selected_processing_id}")
        col_delete, _, col_save = st.columns([1, 6, 1])
        with col_delete:
            delete_submitted = st.button("Verfahren löschen", type="primary", key=f"vvp_edit_delete_{selected_processing_id}")
        with col_save:
            submitted = st.button("Änderungen speichern", key=f"vvp_edit_save_{selected_processing_id}")

        if delete_submitted:
            try:
                client._delete_asset(
                    endpoint=f"/rest/{config.database_name}/processings/{selected_processing_id}",
                    force_delete=True,
                    disable_retries=True,
                )
            except Exception as exc:
                st.session_state[EDIT_ERROR_MESSAGE_KEY] = f"Fehler beim Löschen: {exc}"
                return

            st.session_state.pop(EDIT_ERROR_MESSAGE_KEY, None)
            if "vvp_collection_context" in st.session_state:
                del st.session_state["vvp_collection_context"]
            if "vvp_context_for_abteilung_id" in st.session_state:
                del st.session_state["vvp_context_for_abteilung_id"]
            st.session_state.pop(f"{edit_processing_prefix}_combo", None)
            st.session_state[EDIT_PROCESSING_VERSION_KEY] = edit_processing_version + 1
            set_success_popup("Verfahren gelöscht.")
            st.rerun()
            return

        if not submitted:
            return

        if not label.strip():
            st.session_state[EDIT_ERROR_MESSAGE_KEY] = "Die Bezeichnung darf nicht leer sein."
            return
        if not selected_collection:
            st.session_state[EDIT_ERROR_MESSAGE_KEY] = "Bitte eine verantwortliche Stelle auswählen."
            return

        current_payload = client.build_processing_payload(
            label=form_values["label"],
            in_collection_uuid=form_values["inCollection"],
            legal_foundation=form_values["legalFoundation"],
            legal_foundation_source=form_values["legalFoundationSource"],
            website=form_values["website"],
            data_processing_purpose=form_values["dataProcessingPurpose"],
        )
        payload = client.build_processing_payload(
            label=label,
            in_collection_uuid=selected_collection["id"],
            legal_foundation=legal_foundation,
            legal_foundation_source=legal_foundation_source,
            website=website,
            data_processing_purpose=data_processing_purpose,
        )
        desired_usage_targets = collect_selected_law_target_ids(rows)
        initial_usage_targets = [
            str(item).strip()
            for item in st.session_state.get(EDIT_INITIAL_USAGE_TARGETS_KEY, [])
            if str(item).strip()
        ]
        usage_changed = set(desired_usage_targets) != set(initial_usage_targets)

        if payload == current_payload and not usage_changed:
            st.session_state.pop(EDIT_ERROR_MESSAGE_KEY, None)
            st.warning("Keine Änderungen erkannt. Es wurde nichts gespeichert.")
            return

        if payload != current_payload:
            try:
                client.update_processing(
                    processing_uuid=selected_processing_id,
                    payload=payload,
                    status="PUBLISHED",
                )
            except Exception as exc:
                st.session_state[EDIT_ERROR_MESSAGE_KEY] = f"Fehler beim Speichern: {exc}"
                return
        try:
            client.sync_processing_law_usages(
                processing_uuid=selected_processing_id,
                desired_source_ids=desired_usage_targets,
                law_scheme_id=law_scheme_id,
            )
        except Exception as exc:
            st.session_state[EDIT_ERROR_MESSAGE_KEY] = (
                f"Processing wurde gespeichert, aber Usage-Sync ist fehlgeschlagen: {exc}"
            )
            return

        st.session_state.pop(EDIT_ERROR_MESSAGE_KEY, None)
        if "vvp_collection_context" in st.session_state:
            del st.session_state["vvp_collection_context"]
        if "vvp_context_for_abteilung_id" in st.session_state:
            del st.session_state["vvp_context_for_abteilung_id"]
        st.session_state.pop(f"{edit_processing_prefix}_combo", None)
        st.session_state.pop(EDIT_LAW_ROWS_KEY, None)
        st.session_state.pop(EDIT_PREV_AUTO_URLS_KEY, None)
        st.session_state.pop(EDIT_INITIAL_USAGE_TARGETS_KEY, None)
        st.session_state.pop(EDIT_LAW_ROWS_FOR_PROCESSING_KEY, None)
        st.session_state[EDIT_PROCESSING_VERSION_KEY] = edit_processing_version + 1
        set_success_popup("Verfahren gespeichert.")
        st.rerun()


def render_create_form(client: VVPClient, collection_options: List[Dict[str, Any]]) -> None:
    with st.expander("Neues Verfahren erfassen", expanded=False):
        create_error = str(st.session_state.get(CREATE_ERROR_MESSAGE_KEY, "")).strip()
        if create_error:
            st.error(create_error)

        create_form_version = int(st.session_state.get(CREATE_FORM_VERSION_KEY, 0))
        label_key = f"{CREATE_LABEL_KEY}_{create_form_version}"
        legal_foundation_key = f"{CREATE_LEGAL_FOUNDATION_KEY}_{create_form_version}"
        legal_foundation_source_key = f"{CREATE_LEGAL_FOUNDATION_SOURCE_KEY}_{create_form_version}"
        website_key = f"{CREATE_WEBSITE_KEY}_{create_form_version}"
        purpose_key = f"{CREATE_PURPOSE_KEY}_{create_form_version}"

        selected_collection = searchable_combobox_no_default(
            title="Verantwortliche Stelle",
            options=collection_options,
            widget_prefix="create_collection",
        )

        label = st.text_input("Bezeichnung", key=label_key)
        legal_foundation = st.text_area("Rechtliche Grundlage(n)", key=legal_foundation_key)
        law_context = get_law_context_cached(client)
        law_scheme_id = str(law_context["law_scheme_id"]).strip()
        law_object_options = build_law_object_options(law_context["objects"])
        law_object_lookup = {str(item.get("id")): item for item in law_object_options}

        rows = render_legal_basis_rows(
            client=client,
            rows_state_key=CREATE_LAW_ROWS_KEY,
            widget_prefix=f"create_law_{create_form_version}",
            object_options=law_object_options,
        )
        value_lookup_by_object: Dict[str, Dict[str, Dict[str, Any]]] = {}
        for row in rows:
            object_id = str(row.get("object_id") or "").strip()
            if not object_id or object_id in value_lookup_by_object:
                continue
            object_values = get_law_values_for_object_cached(client=client, object_id=object_id)
            value_lookup_by_object[object_id] = {str(item.get("id")): item for item in object_values}

        sync_source_field_with_selected_urls(
            source_state_key=legal_foundation_source_key,
            previous_auto_urls_state_key=CREATE_PREV_AUTO_URLS_KEY,
            selected_rows=rows,
            object_lookup=law_object_lookup,
            value_lookup_by_object=value_lookup_by_object,
        )
        legal_foundation_source = st.text_area("Quelle(n)", key=legal_foundation_source_key)
        website = st.text_input("Internetauftritt", key=website_key)
        data_processing_purpose = st.text_area("Zweck der Datenbearbeitung", key=purpose_key)
        submitted = st.button("Verfahren erstellen", key=f"vvp_create_submit_{create_form_version}")

        if not submitted:
            return
        if not label.strip():
            st.session_state[CREATE_ERROR_MESSAGE_KEY] = "Die Bezeichnung darf nicht leer sein."
            return
        if not selected_collection:
            st.session_state[CREATE_ERROR_MESSAGE_KEY] = "Bitte eine verantwortliche Stelle auswählen."
            return

        payload = client.build_processing_payload(
            label=label,
            in_collection_uuid=selected_collection["id"],
            legal_foundation=legal_foundation,
            legal_foundation_source=legal_foundation_source,
            website=website,
            data_processing_purpose=data_processing_purpose,
        )
        created_processing = {}
        try:
            created_processing = client.create_processing(
                payload=payload,
                in_collection_uuid=selected_collection["id"],
                status="PUBLISHED",
            )
        except Exception as exc:
            st.session_state[CREATE_ERROR_MESSAGE_KEY] = f"Fehler beim Erstellen: {exc}"
            return
        processing_id = str(created_processing.get("id") or "").strip()
        if not processing_id:
            st.session_state[CREATE_ERROR_MESSAGE_KEY] = "Processing wurde erstellt, aber keine ID zurückgegeben."
            return
        desired_usage_targets = collect_selected_law_target_ids(rows)
        try:
            client.sync_processing_law_usages(
                processing_uuid=processing_id,
                desired_source_ids=desired_usage_targets,
                law_scheme_id=law_scheme_id,
            )
        except Exception as exc:
            st.session_state[CREATE_ERROR_MESSAGE_KEY] = (
                f"Processing wurde erstellt, aber Usage-Sync ist fehlgeschlagen: {exc}"
            )
            return

        st.session_state.pop(CREATE_ERROR_MESSAGE_KEY, None)
        st.session_state.pop(label_key, None)
        st.session_state.pop(legal_foundation_key, None)
        st.session_state.pop(legal_foundation_source_key, None)
        st.session_state.pop(website_key, None)
        st.session_state.pop(purpose_key, None)
        st.session_state[CREATE_FORM_VERSION_KEY] = create_form_version + 1
        if "vvp_collection_context" in st.session_state:
            del st.session_state["vvp_collection_context"]
        if "vvp_context_for_abteilung_id" in st.session_state:
            del st.session_state["vvp_context_for_abteilung_id"]
        st.session_state.pop(CREATE_LAW_ROWS_KEY, None)
        st.session_state.pop(CREATE_PREV_AUTO_URLS_KEY, None)
        set_success_popup("Neues Verfahren wurde erstellt.")
        st.rerun()


def main() -> None:
    st.set_page_config(page_title="VVP - Verfahren mit Personendaten", layout="wide")
    st.title("VVP - Verfahren mit Personendaten")
    st.caption("Erstellen und Bearbeiten von Verfahren innerhalb der gewählten Abteilung.")
    render_success_popup_once()

    client = get_vvp_client()

    departements = get_departements_cached(client)
    departement_options = build_collection_options(departements)
    selected_departement = searchable_combobox_no_default(
        title="Departement",
        options=departement_options,
        widget_prefix="departement",
    )
    if not selected_departement:
        clear_dependent_caches()
        return

    abteilungen = get_abteilungen_cached(client, selected_departement["id"])
    abteilung_options = build_collection_options(abteilungen)
    selected_abteilung = searchable_combobox_no_default(
        title="Abteilung",
        options=abteilung_options,
        widget_prefix="abteilung",
    )
    if not selected_abteilung:
        if "vvp_collection_context" in st.session_state:
            del st.session_state["vvp_collection_context"]
        if "vvp_context_for_abteilung_id" in st.session_state:
            del st.session_state["vvp_context_for_abteilung_id"]
        return

    context = get_collection_context_cached(client, selected_abteilung["id"])
    recursive_collections = context["recursive_collections"]
    processings = context["processings"]
    collection_lookup = context["collection_lookup"]
    collection_options = build_collection_options(recursive_collections)

    render_processing_list(client, processings, collection_lookup)
    render_edit_form(client, processings, collection_options)
    render_create_form(client, collection_options)


if __name__ == "__main__":
    main()
