from typing import Any, Dict, List
import logging

import config
from src.clients.base_client import BaseDataspotClient
from src.common import requests_post_no_retry
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

    def get_vvp_scheme_id(self) -> str:
        query = """
            SELECT s.id
            FROM dataspot.scheme_view s
            WHERE s.label = 'VVP'
              AND s.status = 'PUBLISHED'
        """
        rows = self.execute_query_api(sql_query=query)
        if not rows:
            raise ValueError("VVP-Schema wurde per Query-API nicht gefunden.")
        scheme_id = self._normalize_string(rows[0].get("id")).strip()
        if not scheme_id:
            raise ValueError("VVP-Schema-ID ist leer.")
        logging.info("VVP-Schema-ID per Query-API aufgeloest.")
        return scheme_id

    def get_root_collection(self) -> Dict[str, Any]:
        query = """
            WITH vvp_scheme AS (
                SELECT s.id
                FROM dataspot.scheme_view s
                WHERE s.label = 'VVP'
                  AND s.status = 'PUBLISHED'
            )
            SELECT c.id, c.label, c.in_scheme
            FROM dataspot.collection_view c
            JOIN vvp_scheme s ON c.in_scheme = s.id
            WHERE c.label = 'Regierung und Verwaltung'
              AND c.status = 'PUBLISHED'
        """
        rows = self.execute_query_api(sql_query=query)
        if not rows:
            raise ValueError("Root-Collection 'Regierung und Verwaltung' wurde nicht gefunden.")
        root_collection = rows[0]
        logging.info("Root-Collection im VVP-Schema aufgeloest: %s", root_collection.get("label", ""))
        return root_collection

    def get_child_collections(self, parent_collection_uuid: str) -> List[Dict[str, Any]]:
        query = f"""
            SELECT c.id, c.label, c.parent_id
            FROM dataspot.collection_view c
            WHERE c.parent_id = '{parent_collection_uuid}'
              AND c.status = 'PUBLISHED'
            ORDER BY c.label
        """
        collections = self.execute_query_api(sql_query=query)
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

    def get_recursive_collections_from_query(self, abteilung_uuid: str) -> List[Dict[str, Any]]:
        query = f"""
            WITH RECURSIVE collection_tree AS (
                SELECT c.id, c.label, c.parent_id
                FROM dataspot.collection_view c
                WHERE c.id = '{abteilung_uuid}'
                  AND c.status = 'PUBLISHED'

                UNION ALL

                SELECT child.id, child.label, child.parent_id
                FROM dataspot.collection_view child
                JOIN collection_tree parent ON child.parent_id = parent.id
                WHERE child.status = 'PUBLISHED'
            )
            SELECT id, label, parent_id
            FROM collection_tree
            ORDER BY label
        """
        collections = self.execute_query_api(sql_query=query)
        sorted_collections = sorted(collections, key=lambda item: self._normalize_string(item.get("label")).casefold())
        logging.info("Rekursive Collections per Query-API bestimmt: %s", len(sorted_collections))
        return sorted_collections

    def build_collection_lookup(self, collections: List[Dict[str, Any]]) -> Dict[str, Dict[str, Any]]:
        by_id: Dict[str, Dict[str, Any]] = {}
        for collection in collections:
            collection_id = self._normalize_string(collection.get("id")).strip()
            if not collection_id:
                continue
            by_id[collection_id] = dict(collection)
        logging.info("Collection-Lookup aufgebaut: %s", len(by_id))
        return by_id

    def get_processings_for_collection_tree(self, abteilung_uuid: str) -> List[Dict[str, Any]]:
        query = f"""
            WITH RECURSIVE collection_tree AS (
                SELECT c.id, c.label, c.parent_id
                FROM dataspot.collection_view c
                WHERE c.id = '{abteilung_uuid}'
                  AND c.status = 'PUBLISHED'

                UNION ALL

                SELECT child.id, child.label, child.parent_id
                FROM dataspot.collection_view child
                JOIN collection_tree parent ON child.parent_id = parent.id
                WHERE child.status = 'PUBLISHED'
            ),
            processing_custom_props AS (
                SELECT
                    cp.resource_id,
                    MAX(CASE WHEN cp.name = 'legalFoundation' THEN cp.value END) AS legal_foundation,
                    MAX(CASE WHEN cp.name = 'legalFoundationSource' THEN cp.value END) AS legal_foundation_source,
                    MAX(CASE WHEN cp.name = 'website' THEN cp.value END) AS website,
                    MAX(CASE WHEN cp.name = 'dataProcessingPurpose' THEN cp.value END) AS data_processing_purpose
                FROM dataspot.customproperties_view cp
                WHERE cp.name IN ('legalFoundation', 'legalFoundationSource', 'website', 'dataProcessingPurpose')
                GROUP BY cp.resource_id
            )
            SELECT
                p.id,
                p.label,
                p.in_collection,
                ic.label AS in_collection_label,
                props.legal_foundation,
                props.legal_foundation_source,
                props.website,
                props.data_processing_purpose
            FROM dataspot.processing_view p
            JOIN collection_tree ct ON p.in_collection = ct.id
            LEFT JOIN dataspot.collection_view ic ON ic.id = p.in_collection
            LEFT JOIN processing_custom_props props ON props.resource_id = p.id
            WHERE p.status = 'PUBLISHED'
            ORDER BY p.label
        """
        processings = self.execute_query_api(sql_query=query)
        sorted_processings = sorted(processings, key=lambda item: self._normalize_string(item.get("label")).casefold())
        logging.info("Processings im rekursiven Collection-Teilbaum gefiltert: %s", len(sorted_processings))
        return sorted_processings

    def get_collection_tree_context(self, abteilung_uuid: str) -> Dict[str, Any]:
        recursive_collections = self.get_recursive_collections_from_query(abteilung_uuid)
        processings = self.get_processings_for_collection_tree(abteilung_uuid)
        collection_lookup = self.build_collection_lookup(recursive_collections)
        logging.info(
            "Collection-Context aufgebaut fuer abteilung=%s: %s Collections, %s Processings",
            abteilung_uuid,
            len(recursive_collections),
            len(processings),
        )
        return {
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
        in_collection_value = processing.get("in_collection", processing.get("inCollection"))
        in_collection_label = self._normalize_string(processing.get("in_collection_label")).strip()
        verantwortliche_stelle = in_collection_label
        if not verantwortliche_stelle:
            verantwortliche_stelle = self.resolve_collection_label_for_display(in_collection_value, collection_lookup)
        return {
            "id": self._normalize_string(processing.get("id")),
            "bezeichnung": self._normalize_string(processing.get("label")),
            "rechtliche_grundlage": self._normalize_string(processing.get("legal_foundation", processing.get("legalFoundation"))),
            "quellen": self._normalize_string(processing.get("legal_foundation_source", processing.get("legalFoundationSource"))),
            "internetauftritt": self._normalize_string(processing.get("website")),
            "zweck_datenbearbeitung": self._normalize_string(processing.get("data_processing_purpose", processing.get("dataProcessingPurpose"))),
            "verantwortliche_stelle": verantwortliche_stelle,
        }

    def get_processing_by_uuid(self, processing_uuid: str) -> Dict[str, Any]:
        query = f"""
            WITH processing_custom_props AS (
                SELECT
                    cp.resource_id,
                    MAX(CASE WHEN cp.name = 'legalFoundation' THEN cp.value END) AS legal_foundation,
                    MAX(CASE WHEN cp.name = 'legalFoundationSource' THEN cp.value END) AS legal_foundation_source,
                    MAX(CASE WHEN cp.name = 'website' THEN cp.value END) AS website,
                    MAX(CASE WHEN cp.name = 'dataProcessingPurpose' THEN cp.value END) AS data_processing_purpose
                FROM dataspot.customproperties_view cp
                WHERE cp.name IN ('legalFoundation', 'legalFoundationSource', 'website', 'dataProcessingPurpose')
                GROUP BY cp.resource_id
            )
            SELECT
                p.id,
                p.label,
                p.in_collection,
                props.legal_foundation,
                props.legal_foundation_source,
                props.website,
                props.data_processing_purpose
            FROM dataspot.processing_view p
            LEFT JOIN processing_custom_props props ON props.resource_id = p.id
            WHERE p.id = '{processing_uuid}'
              AND p.status = 'PUBLISHED'
        """
        rows = self.execute_query_api(sql_query=query)
        if not rows:
            raise ValueError(f"Processing nicht gefunden: {processing_uuid}")
        processing = rows[0]
        logging.info("Processing per Query-API geladen: %s", processing_uuid)
        return processing

    def map_rest_processing_to_form(self, processing: Dict[str, Any]) -> Dict[str, Any]:
        return {
            "id": self._normalize_string(processing.get("id")),
            "label": self._normalize_string(processing.get("label")),
            "inCollection": self._normalize_string(processing.get("in_collection", processing.get("inCollection"))),
            "legalFoundation": self._normalize_string(processing.get("legal_foundation", processing.get("legalFoundation"))),
            "legalFoundationSource": self._normalize_string(processing.get("legal_foundation_source", processing.get("legalFoundationSource"))),
            "website": self._normalize_string(processing.get("website")),
            "dataProcessingPurpose": self._normalize_string(processing.get("data_processing_purpose", processing.get("dataProcessingPurpose"))),
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
        url = f"{config.base_url}/rest/{config.database_name}/collections/{in_collection_uuid}/processings"
        data_to_send = dict(payload)
        data_to_send["status"] = status
        created_processing = requests_post_no_retry(
            url=url,
            json=data_to_send,
            headers=self.auth.get_headers(),
        ).json()
        logging.info("Processing erstellt in Collection %s", in_collection_uuid)
        return created_processing

    def update_processing(self, processing_uuid: str, payload: Dict[str, Any], status: str = "PUBLISHED") -> Dict[str, Any]:
        endpoint = f"/rest/{config.database_name}/processings/{processing_uuid}"
        updated_processing = self._update_asset(endpoint=endpoint, data=payload, replace=False, status=status)
        logging.info("Processing aktualisiert: %s", processing_uuid)
        return updated_processing
