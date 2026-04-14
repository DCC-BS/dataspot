from typing import Any, Dict, List
import logging
from urllib.parse import quote

import config
from src.clients.base_client import BaseDataspotClient
from src.common import requests_get
from src.mapping_handlers.org_structure_handler import OrgStructureHandler


class VVPClient(BaseDataspotClient):
    """Client for interacting with the VVP scheme."""

    ROOT_DEPARTMENTS_COLLECTION_LABEL = "Regierung und Verwaltung"

    def __init__(self):
        super().__init__(
            scheme_name=config.vvp_scheme_name,
            scheme_name_short=config.vvp_scheme_name_short,
        )
        self.org_handler = OrgStructureHandler(self)

    @staticmethod
    def _normalize_string(value: Any) -> str:
        if value is None:
            return ""
        return str(value)

    @staticmethod
    def _normalize_custom_property_value(value: Any) -> Any:
        if value is None:
            return None
        if isinstance(value, str):
            stripped = value.strip()
            return stripped if stripped else None
        return value

    @staticmethod
    def _extract_collections(payload: Dict[str, Any]) -> List[Dict[str, Any]]:
        embedded = payload.get("_embedded")
        if isinstance(embedded, dict):
            collections = embedded.get("collections")
            if isinstance(collections, list):
                return collections
        if isinstance(payload, list):
            return payload
        return []

    @staticmethod
    def _build_collection_business_key(collection: Dict[str, Any]) -> str:
        label = VVPClient._normalize_string(collection.get("label")).strip()
        parent_path = VVPClient._normalize_string(collection.get("inCollection")).strip()
        if not label:
            return ""
        if not parent_path:
            return label
        return f"{parent_path}/{label}"

    def get_root_collection(self) -> Dict[str, Any]:
        url = f"{config.base_url}/rest/{config.database_name}/schemes/{quote(self.scheme_name, safe='')}/collections/{quote(self.ROOT_DEPARTMENTS_COLLECTION_LABEL, safe='')}"
        response = requests_get(url, headers=self.auth.get_headers())
        response.raise_for_status()
        root_collection = response.json()
        logging.info("Root-Collection im VVP-Schema aufgeloest: %s", root_collection.get("label", ""))
        return root_collection

    def get_child_collections(self, parent_collection_uuid: str) -> List[Dict[str, Any]]:
        url = f"{config.base_url}/rest/{config.database_name}/collections/{parent_collection_uuid}/collections"
        response = requests_get(url, headers=self.auth.get_headers())
        response.raise_for_status()
        payload = response.json()
        collections = self._extract_collections(payload)
        logging.info("Kind-Collections geladen fuer parent=%s: %s", parent_collection_uuid, len(collections))
        return collections

    def get_departements(self) -> List[Dict[str, Any]]:
        root_collection = self.get_root_collection()
        root_uuid = self._normalize_string(root_collection.get("id")).strip()
        if not root_uuid:
            raise ValueError("Root-Collection hat keine UUID.")
        collections = self.get_child_collections(root_uuid)
        sorted_collections = sorted(collections, key=lambda item: self._normalize_string(item.get("label")).casefold())
        logging.info("Departements geladen: %s", len(sorted_collections))
        return sorted_collections

    def get_abteilungen(self, departement_uuid: str) -> List[Dict[str, Any]]:
        collections = self.get_child_collections(departement_uuid)
        sorted_collections = sorted(collections, key=lambda item: self._normalize_string(item.get("label")).casefold())
        logging.info("Abteilungen geladen fuer departement=%s: %s", departement_uuid, len(sorted_collections))
        return sorted_collections

    def download_assets_for_collection(self, collection_uuid: str) -> List[Dict[str, Any]]:
        url = f"{config.base_url}/api/{config.database_name}/collections/{collection_uuid}/download?format=JSON"
        response = requests_get(url, headers=self.auth.get_headers())
        response.raise_for_status()
        assets = response.json()
        if not isinstance(assets, list):
            raise ValueError(f"Unerwarteter Download-API-Response: {type(assets)}")
        logging.info("Assets ueber Download-API geladen fuer collection=%s: %s", collection_uuid, len(assets))
        return assets

    def get_recursive_collections_from_download(self, downloaded_assets: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        collections = [asset for asset in downloaded_assets if asset.get("_type") == "Collection"]
        sorted_collections = sorted(collections, key=lambda item: self._normalize_string(item.get("label")).casefold())
        logging.info("Rekursive Collections aus Download bestimmt: %s", len(sorted_collections))
        return sorted_collections

    def build_collection_lookup(self, collections: List[Dict[str, Any]]) -> Dict[str, Dict[str, Any]]:
        by_id: Dict[str, Dict[str, Any]] = {}
        for collection in collections:
            collection_id = self._normalize_string(collection.get("id")).strip()
            if not collection_id:
                continue
            enriched_collection = dict(collection)
            enriched_collection["businessKey"] = self._build_collection_business_key(collection)
            by_id[collection_id] = enriched_collection
        logging.info("Collection-Lookup aufgebaut: %s", len(by_id))
        return by_id

    def get_processings_for_collection_tree(
        self,
        downloaded_assets: List[Dict[str, Any]],
        recursive_collections: List[Dict[str, Any]],
    ) -> List[Dict[str, Any]]:
        allowed_collection_ids = {
            self._normalize_string(collection.get("id")).strip()
            for collection in recursive_collections
            if self._normalize_string(collection.get("id")).strip()
        }
        allowed_collection_business_keys = {
            self._build_collection_business_key(collection)
            for collection in recursive_collections
            if self._build_collection_business_key(collection)
        }
        processings = []
        for asset in downloaded_assets:
            if asset.get("_type") != "Processing":
                continue
            in_collection_value = self._normalize_string(asset.get("inCollection")).strip()
            if in_collection_value in allowed_collection_ids or in_collection_value in allowed_collection_business_keys:
                processings.append(asset)
        sorted_processings = sorted(processings, key=lambda item: self._normalize_string(item.get("label")).casefold())
        logging.info("Processings im rekursiven Collection-Teilbaum gefiltert: %s", len(sorted_processings))
        return sorted_processings

    def get_collection_tree_context(self, abteilung_uuid: str) -> Dict[str, Any]:
        downloaded_assets = self.download_assets_for_collection(abteilung_uuid)
        recursive_collections = self.get_recursive_collections_from_download(downloaded_assets)
        processings = self.get_processings_for_collection_tree(downloaded_assets, recursive_collections)
        collection_lookup = self.build_collection_lookup(recursive_collections)
        logging.info(
            "Collection-Context aufgebaut fuer abteilung=%s: %s Collections, %s Processings",
            abteilung_uuid,
            len(recursive_collections),
            len(processings),
        )
        return {
            "downloaded_assets": downloaded_assets,
            "recursive_collections": recursive_collections,
            "collection_lookup": collection_lookup,
            "processings": processings,
        }

    def resolve_collection_label_for_display(self, in_collection_value: Any, collection_lookup: Dict[str, Dict[str, Any]]) -> str:
        raw_value = self._normalize_string(in_collection_value).strip()
        if not raw_value:
            return ""
        if "/" in raw_value:
            return raw_value.split("/")[-1]
        collection = collection_lookup.get(raw_value)
        if collection:
            return self._normalize_string(collection.get("label")).strip()
        return raw_value

    def map_download_processing_to_display(
        self,
        processing: Dict[str, Any],
        collection_lookup: Dict[str, Dict[str, Any]],
    ) -> Dict[str, Any]:
        return {
            "id": self._normalize_string(processing.get("id")),
            "bezeichnung": self._normalize_string(processing.get("label")),
            "rechtliche_grundlage": self._normalize_string(processing.get("legalFoundation")),
            "quellen": self._normalize_string(processing.get("legalFoundationSource")),
            "internetauftritt": self._normalize_string(processing.get("website")),
            "zweck_datenbearbeitung": self._normalize_string(processing.get("dataProcessingPurpose")),
            "verantwortliche_stelle": self.resolve_collection_label_for_display(
                processing.get("inCollection"),
                collection_lookup,
            ),
        }

    def get_processing_by_uuid(self, processing_uuid: str) -> Dict[str, Any]:
        endpoint = f"/rest/{config.database_name}/processings/{processing_uuid}"
        processing = self._get_asset(endpoint)
        if processing is None:
            raise ValueError(f"Processing nicht gefunden: {processing_uuid}")
        logging.info("Processing per REST geladen: %s", processing_uuid)
        return processing

    def map_rest_processing_to_form(self, processing: Dict[str, Any]) -> Dict[str, Any]:
        custom_properties = processing.get("customProperties")
        if not isinstance(custom_properties, dict):
            custom_properties = {}
        return {
            "id": self._normalize_string(processing.get("id")),
            "label": self._normalize_string(processing.get("label")),
            "inCollection": self._normalize_string(processing.get("inCollection")),
            "legalFoundation": self._normalize_string(custom_properties.get("legalFoundation")),
            "legalFoundationSource": self._normalize_string(custom_properties.get("legalFoundationSource")),
            "website": self._normalize_string(custom_properties.get("website")),
            "dataProcessingPurpose": self._normalize_string(custom_properties.get("dataProcessingPurpose")),
        }

    def build_processing_payload(
        self,
        label: str,
        in_collection_uuid: str,
        legal_foundation: str,
        legal_foundation_source: str,
        website: str,
        data_processing_purpose: str,
    ) -> Dict[str, Any]:
        payload = {
            "_type": "Processing",
            "label": self._normalize_string(label).strip(),
            "inCollection": self._normalize_string(in_collection_uuid).strip(),
            "customProperties": {
                "legalFoundation": self._normalize_custom_property_value(legal_foundation),
                "legalFoundationSource": self._normalize_custom_property_value(legal_foundation_source),
                "website": self._normalize_custom_property_value(website),
                "dataProcessingPurpose": self._normalize_custom_property_value(data_processing_purpose),
            },
        }
        return payload

    def create_processing(self, payload: Dict[str, Any], in_collection_uuid: str, status: str = "PUBLISHED") -> Dict[str, Any]:
        endpoint = f"/rest/{config.database_name}/collections/{in_collection_uuid}/processings"
        created_processing = self._create_asset(endpoint=endpoint, data=payload, status=status)
        logging.info("Processing erstellt in Collection %s", in_collection_uuid)
        return created_processing

    def update_processing(self, processing_uuid: str, payload: Dict[str, Any], status: str = "PUBLISHED") -> Dict[str, Any]:
        endpoint = f"/rest/{config.database_name}/processings/{processing_uuid}"
        updated_processing = self._update_asset(endpoint=endpoint, data=payload, replace=False, status=status)
        logging.info("Processing aktualisiert: %s", processing_uuid)
        return updated_processing
