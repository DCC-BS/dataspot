from typing import Any, Dict, List
import logging
import re
from urllib.parse import urlparse

import config
from src.clients.base_client import BaseDataspotClient
from src.clients.law_client import LAWClient
from src.clients.helpers import normalize_multiline_markdown, prepare_custom_property_for_form
from src.common import requests_delete_no_retry, requests_get, requests_patch_no_retry, requests_post_no_retry
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
        self.law_client = LAWClient()

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
            return normalize_multiline_markdown(value)
        return value

    @staticmethod
    def _normalize_url_key(value: str) -> str:
        return str(value or "").strip().casefold()

    @staticmethod
    def _extract_url_from_description(value: Any) -> str:
        raw = str(value or "").strip()
        if not raw:
            return ""
        parsed = urlparse(raw)
        if parsed.scheme not in {"http", "https"} or not parsed.netloc:
            logging.info("Ignored invalid or missing URL in LAW description.")
            return ""
        return raw

    @staticmethod
    def _natural_sort_key(value: Any) -> List[Any]:
        text = str(value or "").strip().casefold()
        parts = re.split(r"(\d+)", text)
        key: List[Any] = []
        for part in parts:
            if part.isdigit():
                key.append(int(part))
            else:
                key.append(part)
        return key

    def get_law_reference_objects(self) -> List[Dict[str, Any]]:
        law_scheme_id = self.law_client.get_scheme_id()
        query = f"""
            SELECT e.id, e.label, e.description, e.in_collection
            FROM dataspot.enumeration_view e
            JOIN dataspot.collection_view c ON c.id = e.in_collection
            WHERE c.in_scheme = '{law_scheme_id}'::uuid
              AND e.status = 'PUBLISHED'
            ORDER BY e.label
        """
        rows = self.execute_query_api(sql_query=query)
        objects: List[Dict[str, Any]] = []
        for row in rows:
            object_id = self._normalize_string(row.get("id")).strip()
            label = self._normalize_string(row.get("label")).strip()
            if not object_id or not label:
                continue
            objects.append(
                {
                    "id": object_id,
                    "label": label,
                    "description": self._normalize_string(row.get("description")),
                    "source_url": self._extract_url_from_description(row.get("description")),
                }
            )
        logging.info("Loaded LAW reference objects: %s", len(objects))
        return objects

    def get_law_reference_values_by_object(self, object_id: str) -> List[Dict[str, Any]]:
        normalized_object_id = self._normalize_string(object_id).strip()
        if not normalized_object_id:
            return []

        literals_url = (
            f"{config.base_url}/rest/{config.database_name}/enumerations/{normalized_object_id}/literals"
        )
        response = requests_get(
            url=literals_url,
            headers=self.auth.get_headers(),
            skip_sleep=True,
        )
        payload = response.json()
        raw_literals: List[Dict[str, Any]] = []
        if isinstance(payload, dict):
            embedded = payload.get("_embedded", {})
            if isinstance(embedded, dict):
                raw_literals = embedded.get("literals", []) or []
        if not isinstance(raw_literals, list):
            raw_literals = []
        rows = [row for row in raw_literals if isinstance(row, dict)]
        values: List[Dict[str, Any]] = []
        for row in rows:
            status = self._normalize_string(row.get("status")).strip()
            if status and status != "PUBLISHED":
                continue
            value_id = self._normalize_string(row.get("id")).strip()
            label = self._normalize_string(row.get("label")).strip()
            if not value_id or not label:
                continue
            values.append(
                {
                    "id": value_id,
                    "literal_of": self._normalize_string(
                        row.get("literal_of", row.get("literalOf"))
                    ).strip(),
                    "label": label,
                    "description": self._normalize_string(row.get("description")),
                    "source_url": self._extract_url_from_description(row.get("description")),
                }
            )
        sorted_values = sorted(values, key=lambda item: self._natural_sort_key(item.get("label")))
        logging.info("Loaded LAW reference values for object=%s: %s", normalized_object_id, len(sorted_values))
        return sorted_values

    def get_law_reference_values_by_ids(self, value_ids: List[str]) -> List[Dict[str, Any]]:
        normalized_value_ids = [
            self._normalize_string(value_id).strip()
            for value_id in value_ids
            if self._normalize_string(value_id).strip()
        ]
        if not normalized_value_ids:
            return []
        value_uuid_list = ", ".join(f"'{value_id}'::uuid" for value_id in normalized_value_ids)
        query = f"""
            SELECT l.id, l.literal_of, l.label, l.description
            FROM dataspot.literal_view l
            WHERE l.id IN ({value_uuid_list})
              AND l.status = 'PUBLISHED'
        """
        rows = self.execute_query_api(sql_query=query)
        values: List[Dict[str, Any]] = []
        for row in rows:
            value_id = self._normalize_string(row.get("id")).strip()
            parent_object_id = self._normalize_string(row.get("literal_of")).strip()
            label = self._normalize_string(row.get("label")).strip()
            if not value_id or not parent_object_id or not label:
                continue
            values.append(
                {
                    "id": value_id,
                    "literal_of": parent_object_id,
                    "label": label,
                    "description": self._normalize_string(row.get("description")),
                    "source_url": self._extract_url_from_description(row.get("description")),
                }
            )
        logging.info("Loaded LAW reference values by IDs: %s", len(values))
        return values

    def get_processing_usages(self, processing_uuid: str, law_scheme_id: str) -> List[Dict[str, Any]]:
        normalized_processing_uuid = self._normalize_string(processing_uuid).strip()
        normalized_law_scheme_id = self._normalize_string(law_scheme_id).strip()
        if not normalized_processing_uuid or not normalized_law_scheme_id:
            return []

        query = f"""
            SELECT u.id, u.resource_id, u.usage_of, u.qualifier
            FROM dataspot.usageof_view u
            WHERE u.resource_id = '{normalized_processing_uuid}'::uuid
              AND u.model_id = '{normalized_law_scheme_id}'::uuid
        """
        rows = self.execute_query_api(sql_query=query)
        logging.info("Loaded processing usages for processing=%s: %s", normalized_processing_uuid, len(rows))
        return rows

    def get_processing_usage_targets(self, processing_uuid: str) -> List[Dict[str, Any]]:
        normalized_processing_uuid = self._normalize_string(processing_uuid).strip()
        if not normalized_processing_uuid:
            return []

        url = f"{config.base_url}/rest/{config.database_name}/processings/{normalized_processing_uuid}/usageOf"
        response = requests_get(
            url=url,
            headers=self.auth.get_headers(),
            skip_sleep=True,
        )
        payload = response.json()
        usage_rows: List[Dict[str, Any]] = []
        if isinstance(payload, dict):
            embedded = payload.get("_embedded", {})
            if isinstance(embedded, dict):
                raw_rows = embedded.get("usageOf", [])
                if isinstance(raw_rows, list):
                    usage_rows = [row for row in raw_rows if isinstance(row, dict)]

        targets: List[Dict[str, Any]] = []
        for row in usage_rows:
            usage_id = self._normalize_string(row.get("id")).strip()
            usage_of = self._normalize_string(row.get("usageOf", row.get("usage_of"))).strip()
            if not usage_id or not usage_of:
                continue
            targets.append(
                {
                    "id": usage_id,
                    "usage_of": usage_of,
                    "used_by": self._normalize_string(row.get("usedBy", row.get("used_by"))).strip(),
                    "model_id": self._normalize_string(row.get("modelId", row.get("model_id"))).strip(),
                }
            )
        logging.info(
            "Loaded processing usage targets via REST for processing=%s: %s",
            normalized_processing_uuid,
            len(targets),
        )
        return targets

    def get_asset_by_uuid(self, asset_uuid: str) -> Dict[str, Any]:
        normalized_asset_uuid = self._normalize_string(asset_uuid).strip()
        if not normalized_asset_uuid:
            return {}

        url = f"{config.base_url}/rest/{config.database_name}/assets/{normalized_asset_uuid}"
        response = requests_get(
            url=url,
            headers=self.auth.get_headers(),
            skip_sleep=True,
        )
        payload = response.json()
        if not isinstance(payload, dict):
            raise ValueError(
                f"Unexpected asset response type for {normalized_asset_uuid}: {type(payload)}"
            )

        asset = {
            "id": self._normalize_string(payload.get("id")).strip(),
            "_type": self._normalize_string(payload.get("_type")).strip(),
            "label": self._normalize_string(payload.get("label")).strip(),
            "literal_of": self._normalize_string(payload.get("literalOf", payload.get("literal_of"))).strip(),
            "model_id": self._normalize_string(payload.get("modelId", payload.get("model_id"))).strip(),
            "status": self._normalize_string(payload.get("status")).strip(),
            "description": self._normalize_string(payload.get("description")),
            "source_url": self._extract_url_from_description(payload.get("description")),
        }
        logging.info(
            "Loaded asset via REST id=%s type=%s status=%s",
            normalized_asset_uuid,
            asset["_type"],
            asset["status"],
        )
        return asset

    def create_usage(self, used_by_processing_uuid: str, usage_of_uuid: str) -> Dict[str, Any]:
        url = f"{config.base_url}/rest/{config.database_name}/usages"
        payload = {
            "_type": "Usage",
            "usedBy": self._normalize_string(used_by_processing_uuid).strip(),
            "usageOf": self._normalize_string(usage_of_uuid).strip(),
        }
        created_usage = requests_post_no_retry(
            url=url,
            json=payload,
            headers=self.auth.get_headers(),
            skip_sleep=True,
        ).json()
        logging.info(
            "Created usage for processing=%s usageOf=%s",
            payload["usedBy"],
            payload["usageOf"],
        )
        return created_usage

    def update_usage(self, usage_uuid: str, usage_of_uuid: str) -> Dict[str, Any]:
        url = f"{config.base_url}/rest/{config.database_name}/usages/{usage_uuid}"
        payload = {
            "_type": "Usage",
            "usageOf": self._normalize_string(usage_of_uuid).strip(),
        }
        updated_usage = requests_patch_no_retry(
            url=url,
            json=payload,
            headers=self.auth.get_headers(),
            skip_sleep=True,
        ).json()
        logging.info("Updated usage id=%s usageOf=%s", usage_uuid, payload["usageOf"])
        return updated_usage

    def delete_usage(self, usage_uuid: str) -> None:
        url = f"{config.base_url}/rest/{config.database_name}/usages/{usage_uuid}"
        requests_delete_no_retry(
            url=url,
            headers=self.auth.get_headers(),
            skip_sleep=True,
        )
        logging.info("Deleted usage id=%s", usage_uuid)

    def sync_processing_law_usages(
        self,
        processing_uuid: str,
        desired_source_ids: List[str],
        law_scheme_id: str,
    ) -> Dict[str, Any]:
        normalized_processing_uuid = self._normalize_string(processing_uuid).strip()
        normalized_law_scheme_id = self._normalize_string(law_scheme_id).strip()
        if not normalized_processing_uuid:
            raise ValueError("processing_uuid darf nicht leer sein.")
        if not normalized_law_scheme_id:
            raise ValueError("law_scheme_id darf nicht leer sein.")

        current_rows = self.get_processing_usages(
            processing_uuid=normalized_processing_uuid,
            law_scheme_id=normalized_law_scheme_id,
        )
        usage_id_to_source_id: Dict[str, str] = {}
        for row in current_rows:
            usage_id = self._normalize_string(row.get("id")).strip()
            source_id = self._normalize_string(row.get("usage_of")).strip()
            if usage_id and source_id:
                usage_id_to_source_id[usage_id] = source_id

        desired_set = {
            self._normalize_string(source_id).strip()
            for source_id in desired_source_ids
            if self._normalize_string(source_id).strip()
        }
        current_set = set(usage_id_to_source_id.values())
        usage_ids_to_delete = [
            usage_id
            for usage_id, source_id in usage_id_to_source_id.items()
            if source_id not in desired_set
        ]
        source_ids_to_create = sorted(desired_set - current_set)

        for usage_id in usage_ids_to_delete:
            self.delete_usage(usage_uuid=usage_id)
        for source_id in source_ids_to_create:
            self.create_usage(
                used_by_processing_uuid=normalized_processing_uuid,
                usage_of_uuid=source_id,
            )

        summary = {
            "deleted": len(usage_ids_to_delete),
            "created": len(source_ids_to_create),
            "desired": len(desired_set),
            "existing": len(current_set),
        }
        logging.info(
            "Completed usage sync for processing=%s (created=%s, deleted=%s).",
            normalized_processing_uuid,
            summary["created"],
            summary["deleted"],
        )
        return summary

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
        logging.info("Resolved VVP scheme ID via Query API.")
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
        logging.info("Resolved root collection in VVP scheme: %s", root_collection.get("label", ""))
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
        logging.info("Loaded child collections for parent=%s: %s", parent_collection_uuid, len(collections))
        return collections

    def get_departements(self) -> List[Dict[str, Any]]:
        root_collection = self.get_root_collection()
        root_uuid = self._normalize_string(root_collection.get("id")).strip()
        if not root_uuid:
            raise ValueError("Root-Collection hat keine UUID.")
        collections = self.get_child_collections(root_uuid)
        sorted_collections = sorted(collections, key=lambda item: self._normalize_string(item.get("label")).casefold())
        logging.info("Loaded departments: %s", len(sorted_collections))
        return sorted_collections

    def get_abteilungen(self, departement_uuid: str) -> List[Dict[str, Any]]:
        collections = self.get_child_collections(departement_uuid)
        sorted_collections = sorted(collections, key=lambda item: self._normalize_string(item.get("label")).casefold())
        logging.info("Loaded divisions for department=%s: %s", departement_uuid, len(sorted_collections))
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
        logging.info("Resolved recursive collections via Query API: %s", len(sorted_collections))
        return sorted_collections

    def build_collection_lookup(self, collections: List[Dict[str, Any]]) -> Dict[str, Dict[str, Any]]:
        by_id: Dict[str, Dict[str, Any]] = {}
        for collection in collections:
            collection_id = self._normalize_string(collection.get("id")).strip()
            if not collection_id:
                continue
            by_id[collection_id] = dict(collection)
        logging.info("Built collection lookup: %s", len(by_id))
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
        logging.info("Filtered processings in recursive collection subtree: %s", len(sorted_processings))
        return sorted_processings

    def get_collection_tree_context(self, abteilung_uuid: str) -> Dict[str, Any]:
        recursive_collections = self.get_recursive_collections_from_query(abteilung_uuid)
        processings = self.get_processings_for_collection_tree(abteilung_uuid)
        collection_lookup = self.build_collection_lookup(recursive_collections)
        logging.info(
            "Built collection context for division=%s: %s collections, %s processings",
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
        logging.info("Loaded processing via Query API: %s", processing_uuid)
        return processing

    def map_rest_processing_to_form(self, processing: Dict[str, Any]) -> Dict[str, Any]:
        legal_foundation_raw = self._normalize_string(processing.get("legal_foundation", processing.get("legalFoundation")))
        legal_foundation_source_raw = self._normalize_string(processing.get("legal_foundation_source", processing.get("legalFoundationSource")))
        website_raw = self._normalize_string(processing.get("website"))
        data_processing_purpose_raw = self._normalize_string(
            processing.get("data_processing_purpose", processing.get("dataProcessingPurpose"))
        )
        mapped = {
            "id": self._normalize_string(processing.get("id")),
            "label": self._normalize_string(processing.get("label")),
            "inCollection": self._normalize_string(processing.get("in_collection", processing.get("inCollection"))),
            "legalFoundation": prepare_custom_property_for_form(legal_foundation_raw),
            "legalFoundationSource": prepare_custom_property_for_form(legal_foundation_source_raw),
            "website": prepare_custom_property_for_form(website_raw),
            "dataProcessingPurpose": prepare_custom_property_for_form(data_processing_purpose_raw),
        }
        return mapped

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
            skip_sleep=True,
        ).json()
        logging.info("Created processing in collection %s", in_collection_uuid)
        return created_processing

    def update_processing(self, processing_uuid: str, payload: Dict[str, Any], status: str = "PUBLISHED") -> Dict[str, Any]:
        url = f"{config.base_url}/rest/{config.database_name}/processings/{processing_uuid}"
        data_to_send = dict(payload)
        data_to_send["status"] = status
        updated_processing = requests_patch_no_retry(
            url=url,
            json=data_to_send,
            headers=self.auth.get_headers(),
            skip_sleep=True,
        ).json()
        logging.info("Updated processing: %s", processing_uuid)
        return updated_processing
