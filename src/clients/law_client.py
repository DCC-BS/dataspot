from typing import Any, Dict, List, Set
import logging
from urllib.parse import quote

import config
from src.clients.base_client import BaseDataspotClient
from src.clients.helpers import url_join
from src.common import requests_get, requests_post


class LAWClient(BaseDataspotClient):
    """Client for Basel-Stadt law synchronization in reference data."""

    def __init__(self):
        super().__init__(
            scheme_name=config.law_scheme_name,
            scheme_name_short=config.law_scheme_name_short,
        )
        self.collections_cache: Dict[str, str] = {}

    def download_scheme_assets(self) -> List[Dict[str, Any]]:
        """
        Download all assets in the configured LAW scheme as JSON.
        """
        download_url = (
            f"{config.base_url}/api/{config.database_name}/schemes/"
            f"{config.law_scheme_uuid}/download?format=JSON"
        )
        response = requests_get(download_url, headers=self.auth.get_headers())
        response.raise_for_status()
        assets = response.json()
        if not isinstance(assets, list):
            raise ValueError(
                f"Unexpected Download API response type: {type(assets)}. Expected list."
            )
        logging.info(f"Downloaded {len(assets)} assets from LAW scheme via Download API")
        return assets

    def resolve_collection_uuid_by_label(self, collection_label: str) -> str:
        """
        Resolve collection UUID by exact label from the collection endpoint.
        """
        cached_uuid = self.collections_cache.get(collection_label)
        if cached_uuid:
            return cached_uuid

        scheme_name_encoded = quote(config.law_scheme_name, safe="")
        collection_label_encoded = quote(collection_label, safe="")
        collection_url = (
            f"{config.base_url}/rest/{config.database_name}/schemes/"
            f"{scheme_name_encoded}/collections/{collection_label_encoded}"
        )

        response = requests_get(collection_url, headers=self.auth.get_headers())
        response.raise_for_status()
        collection = response.json()
        if not isinstance(collection, dict):
            raise ValueError(
                f"Unexpected collection response type: {type(collection)}. Expected dict."
            )

        collection_id = collection.get("id")
        if not collection_id:
            raise ValueError(
                f"Collection '{collection_label}' not found in scheme '{config.law_scheme_name}'"
            )

        self.collections_cache[collection_label] = str(collection_id)
        logging.info("Resolved LAW collection UUID for label=%s", collection_label)
        return str(collection_id)

    def create_reference_object(
        self, collection_uuid: str, data: Dict[str, Any], status: str = "WORKING"
    ) -> Dict[str, Any]:
        endpoint = f"/rest/{config.database_name}/collections/{collection_uuid}/enumerations"
        return self._create_asset(endpoint=endpoint, data=data, status=status)

    def create_reference_object_deployment(self, law_id: str, systematic_number: str) -> bool:
        """
        Create a Deployment linking a LAW ReferenceObject to the configured System.

        Args:
            law_id: UUID of the created ReferenceObject (enumeration).
            systematic_number: Systematic number of the law (for logging).

        Returns:
            True if deployment was created, False on error.
        """
        deployment_url = f"{config.base_url}/rest/{config.database_name}/deployments"
        payload = {
            "_type": "Deployment",
            "deploymentOf": law_id,
            "deployedIn": config.law_bs_system_uuid,
            "qualifier": "GOLD",
            "order": 1,
            "favorite": True,
        }
        try:
            response = requests_post(
                url=deployment_url,
                json=payload,
                headers=self.auth.get_headers(),
            )
            response.raise_for_status()
            logging.info(
                f"Created LAW system deployment for law systematic_number={systematic_number} ({law_id})"
            )
            return True
        except Exception as e:
            logging.error(
                f"Error creating LAW system deployment for systematic_number={systematic_number} "
                f"law_id={law_id}: {str(e)}"
            )
            return False

    def update_reference_object(
        self, law_id: str, data: Dict[str, Any], status: str = "WORKING"
    ) -> Dict[str, Any]:
        endpoint = f"/rest/{config.database_name}/enumerations/{law_id}"
        return self._update_asset(endpoint=endpoint, data=data, replace=False, status=status)

    def create_reference_value(
        self, law_id: str, data: Dict[str, Any], status: str = "WORKING"
    ) -> Dict[str, Any]:
        endpoint = f"/rest/{config.database_name}/enumerations/{law_id}/literals"
        return self._create_asset(endpoint=endpoint, data=data, status=status)

    def update_reference_value(
        self, value_id: str, data: Dict[str, Any], status: str = "WORKING"
    ) -> Dict[str, Any]:
        endpoint = f"/rest/{config.database_name}/literals/{value_id}"
        return self._update_asset(endpoint=endpoint, data=data, replace=False, status=status)

    def is_parent_in_use(self, enum_uuid: str) -> bool:
        """
        Check whether the ReferenceObject (enumeration) appears in derivedfrom_view.derived_from.
        Returns True if the parent is in use (has derivations).
        """
        query = (
            "SELECT DISTINCT d.derived_from "
            "FROM dataspot.derivedfrom_view d "
            f"WHERE d.derived_from = '{enum_uuid}'::uuid"
        )
        results = self.execute_query_api(query)
        return len(results) > 0

    def get_child_literal_ids_in_use(self, enum_uuid: str) -> Set[str]:
        """
        Return the set of child ReferenceValue (literal) UUIDs that appear in derivedfrom_view.derived_from.
        """
        query = (
            "SELECT DISTINCT l.id AS literal_id "
            "FROM dataspot.literal_view l "
            "JOIN dataspot.derivedfrom_view d ON d.derived_from = l.id "
            f"WHERE l.literal_of = '{enum_uuid}'::uuid"
        )
        results = self.execute_query_api(query)
        ids: Set[str] = set()
        for row in results:
            lit_id = row.get("literal_id")
            if lit_id:
                ids.add(str(lit_id))
        return ids

    def delete_literal(self, literal_id: str) -> None:
        """Permanently delete a ReferenceValue (literal)."""
        endpoint = url_join(
            "rest", config.database_name, "literals", literal_id, leading_slash=True
        )
        self._delete_asset(endpoint, force_delete=True)

    def mark_literal_for_deletion(self, literal_id: str) -> None:
        """Mark a ReferenceValue (literal) for deletion review (REVIEWDCC2)."""
        endpoint = url_join(
            "rest", config.database_name, "literals", literal_id, leading_slash=True
        )
        self.set_asset_status(endpoint=endpoint, status="REVIEWDCC2")

    def delete_reference_object(self, enum_id: str) -> None:
        """Permanently delete a ReferenceObject (enumeration)."""
        endpoint = url_join(
            "rest", config.database_name, "enumerations", enum_id, leading_slash=True
        )
        self._delete_asset(endpoint, force_delete=True)

    def mark_reference_object_for_deletion(self, enum_id: str, status: str = "DELETENEW") -> None:
        """Mark a ReferenceObject (enumeration) for deletion review."""
        endpoint = url_join(
            "rest", config.database_name, "enumerations", enum_id, leading_slash=True
        )
        self._mark_asset_for_deletion(endpoint, status=status)
