from typing import Any, Dict, List
import logging

import config
from src.clients.base_client import BaseDataspotClient
from src.common import requests_get


class LAWClient(BaseDataspotClient):
    """Client for Basel-Stadt law synchronization in reference data."""

    def __init__(self):
        super().__init__(
            scheme_name=config.law_scheme_name,
            scheme_name_short=config.law_scheme_name_short,
        )

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

    def resolve_collection_uuid_by_label(
        self, assets: List[Dict[str, Any]], collection_label: str
    ) -> str:
        """
        Resolve collection UUID by exact label from Download API assets.
        """
        for asset in assets:
            if asset.get("_type") == "Collection" and asset.get("label") == collection_label:
                collection_id = asset.get("id")
                if collection_id:
                    return collection_id
        raise ValueError(
            f"Collection '{collection_label}' not found in scheme '{config.law_scheme_uuid}'"
        )

    def create_reference_object(
        self, collection_uuid: str, data: Dict[str, Any], status: str = "WORKING"
    ) -> Dict[str, Any]:
        endpoint = f"/rest/{config.database_name}/collections/{collection_uuid}/enumerations"
        return self._create_asset(endpoint=endpoint, data=data, status=status)

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

    @staticmethod
    def get_literal_parent_id(asset: Dict[str, Any]) -> str | None:
        """
        Extract literal parent id from known relationship keys.
        """
        parent_id = asset.get("literalOf")
        if parent_id:
            return parent_id
        return asset.get("literal_of")

    @staticmethod
    def get_custom_property(asset: Dict[str, Any], key: str) -> Any:
        custom_props = asset.get("customProperties")
        if not isinstance(custom_props, dict):
            return None
        return custom_props.get(key)

