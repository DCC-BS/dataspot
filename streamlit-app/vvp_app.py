from difflib import SequenceMatcher
from pathlib import Path
from typing import Any, Dict, List
import sys

import streamlit as st


PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.append(str(PROJECT_ROOT))

from src.clients.vvp_client import VVPClient


def normalize_text(value: Any) -> str:
    if value is None:
        return ""
    return str(value).strip().casefold()


def extract_leaf_label(value: Any) -> str:
    raw = str(value or "").strip()
    if "/" in raw:
        return raw.split("/")[-1].strip()
    return raw


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


def render_processing_list(client: VVPClient, processings: List[Dict[str, Any]], collection_lookup: Dict[str, Dict[str, Any]]) -> None:
    st.subheader("Vorhandene Verfahren mit Personendaten")
    if not processings:
        st.info("In der gewählten Abteilung wurden keine Verfahren gefunden.")
        return

    display_rows = [
        client.map_download_processing_to_display(processing, collection_lookup)
        for processing in processings
    ]
    st.dataframe(display_rows, use_container_width=True)


def render_edit_form(
    client: VVPClient,
    processings: List[Dict[str, Any]],
    collection_options: List[Dict[str, Any]],
) -> None:
    st.subheader("Bestehendes Verfahren bearbeiten")
    if not processings:
        st.info("Es gibt aktuell kein Verfahren zum Bearbeiten.")
        return

    processing_options = sorted(
        [{"id": str(item.get("id", "")), "label": str(item.get("label", "")).strip()} for item in processings if item.get("id") and item.get("label")],
        key=lambda item: item["label"].casefold(),
    )
    selected_processing = searchable_combobox_no_default(
        title="Verfahren auswählen",
        options=processing_options,
        widget_prefix="edit_processing",
    )
    if not selected_processing:
        return
    selected_processing_id = selected_processing["id"]

    rest_processing = client.get_processing_by_uuid(selected_processing_id)
    form_values = client.map_rest_processing_to_form(rest_processing)

    with st.form("edit_processing_form"):
        selected_collection = searchable_combobox_no_default(
            title="Verantwortliche Stelle",
            options=collection_options,
            widget_prefix="edit_collection",
            selected_id=form_values["inCollection"],
        )
        label = st.text_input("Bezeichnung", value=form_values["label"])
        legal_foundation = st.text_area("Rechtliche Grundlage(n)", value=form_values["legalFoundation"])
        legal_foundation_source = st.text_area("Quelle(n)", value=form_values["legalFoundationSource"])
        website = st.text_input("Internetauftritt", value=form_values["website"])
        data_processing_purpose = st.text_area("Zweck der Datenbearbeitung", value=form_values["dataProcessingPurpose"])
        submitted = st.form_submit_button("Änderungen speichern")

    if not submitted:
        return
    if not label.strip():
        st.error("Die Bezeichnung darf nicht leer sein.")
        return
    if not selected_collection:
        st.error("Bitte eine verantwortliche Stelle auswählen.")
        return

    payload = client.build_processing_payload(
        label=label,
        in_collection_uuid=selected_collection["id"],
        legal_foundation=legal_foundation,
        legal_foundation_source=legal_foundation_source,
        website=website,
        data_processing_purpose=data_processing_purpose,
    )
    client.update_processing(
        processing_uuid=selected_processing_id,
        payload=payload,
        status="PUBLISHED",
    )
    if str(form_values["inCollection"]) != str(selected_collection["id"]):
        st.success("Verfahren gespeichert und innerhalb der gewählten Abteilung verschoben.")
    else:
        st.success("Verfahren gespeichert.")


def render_create_form(client: VVPClient, collection_options: List[Dict[str, Any]]) -> None:
    st.subheader("Neues Verfahren erstellen")
    with st.form("create_processing_form"):
        selected_collection = searchable_combobox_no_default(
            title="Verantwortliche Stelle",
            options=collection_options,
            widget_prefix="create_collection",
        )
        label = st.text_input("Bezeichnung")
        legal_foundation = st.text_area("Rechtliche Grundlage(n)")
        legal_foundation_source = st.text_area("Quelle(n)")
        website = st.text_input("Internetauftritt")
        data_processing_purpose = st.text_area("Zweck der Datenbearbeitung")
        submitted = st.form_submit_button("Verfahren erstellen")

    if not submitted:
        return
    if not label.strip():
        st.error("Die Bezeichnung darf nicht leer sein.")
        return
    if not selected_collection:
        st.error("Bitte eine verantwortliche Stelle auswählen.")
        return

    payload = client.build_processing_payload(
        label=label,
        in_collection_uuid=selected_collection["id"],
        legal_foundation=legal_foundation,
        legal_foundation_source=legal_foundation_source,
        website=website,
        data_processing_purpose=data_processing_purpose,
    )
    client.create_processing(
        payload=payload,
        in_collection_uuid=selected_collection["id"],
        status="PUBLISHED",
    )
    st.success("Neues Verfahren wurde erstellt.")


def main() -> None:
    st.set_page_config(page_title="VVP - Verfahren mit Personendaten", layout="wide")
    st.title("VVP - Verfahren mit Personendaten")
    st.caption("Erstellen und Bearbeiten von Verfahren innerhalb der gewählten Abteilung.")

    client = get_vvp_client()

    departements = client.get_departements()
    departement_options = build_collection_options(departements)
    selected_departement = searchable_combobox_no_default(
        title="Departement",
        options=departement_options,
        widget_prefix="departement",
    )
    if not selected_departement:
        return

    abteilungen = client.get_abteilungen(selected_departement["id"])
    abteilung_options = build_collection_options(abteilungen)
    selected_abteilung = searchable_combobox_no_default(
        title="Abteilung",
        options=abteilung_options,
        widget_prefix="abteilung",
    )
    if not selected_abteilung:
        return

    context = client.get_collection_tree_context(selected_abteilung["id"])
    recursive_collections = context["recursive_collections"]
    processings = context["processings"]
    collection_lookup = context["collection_lookup"]
    collection_options = build_collection_options(recursive_collections)

    render_processing_list(client, processings, collection_lookup)
    render_edit_form(client, processings, collection_options)
    render_create_form(client, collection_options)


if __name__ == "__main__":
    main()
