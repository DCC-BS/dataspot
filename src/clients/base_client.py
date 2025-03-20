from typing import Dict, Any

from src import config
from src.dataspot_auth import DataspotAuth
from src.common import requests_get, requests_delete, requests_post, requests_put, requests_patch

from requests import HTTPError

# TODO (large language model): If possible, remove rate limiting from this class, and handle that in the common module.
class BaseDataspotClient:
    """Base class for Dataspot API clients with common functionality."""

    def __init__(self, request_delay=1.0, uuid_cache_path="dataspot_uuids.csv"):
        """
        Initialize the DataspotClient with the necessary credentials and configurations.

        Args:
            request_delay (float, optional): The delay between API requests in seconds. Default is 1.0 second.
                                            This helps prevent overloading the server with too many requests.
            uuid_cache_path (str, optional): Path to the CSV file used to cache UUIDs. Default is "dataspot_uuids.csv".
                                            Set to None to disable UUID caching.
        """
        self.auth = DataspotAuth()
        self.request_delay = request_delay

        # Load configuration from config.py
        self.base_url = config.base_url
        self.database_name = config.database_name
        self.dnk_scheme_name = config.dnk_scheme_name
        self.rdm_scheme_name = config.rdm_scheme_name
        self.datatype_scheme_name = config.datatype_scheme_name
        self.tdm_scheme_name = config.tdm_scheme_name
        self.ods_imports_collection_name = config.ods_imports_collection_name
        

    def create_resource(self, endpoint: str, data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Create a new resource via POST request.
        
        Args:
            endpoint (str): Full API endpoint URL
            data (Dict[str, Any]): JSON data for the resource
            
        Returns:
            Dict[str, Any]: JSON response from the API
            
        Raises:
            HTTPError: If the request fails
        """
        headers = self.auth.get_headers()
        response = requests_post(endpoint, headers=headers, json=data, rate_limit_delay=self.request_delay)
        return response.json()
    
    def update_resource(self, endpoint: str, data: Dict[str, Any], replace: bool = False) -> Dict[str, Any]:
        """
        Update an existing resource via PUT or PATCH request.
        
        Args:
            endpoint (str): Full API endpoint URL
            data (Dict[str, Any]): JSON data for the resource
            replace (bool): Whether to completely replace (PUT) or partially update (PATCH)
            
        Returns:
            Dict[str, Any]: JSON response from the API
            
        Raises:
            HTTPError: If the request fails
        """
        headers = self.auth.get_headers()
        
        if replace:
            # Use PUT to completely replace the resource
            response = requests_put(endpoint, headers=headers, json=data, rate_limit_delay=self.request_delay)
        else:
            # Use PATCH to update only the specified properties
            response = requests_patch(endpoint, headers=headers, json=data, rate_limit_delay=self.request_delay)
            
        return response.json()
    
    def delete_resource(self, endpoint: str) -> None:
        """
        Delete a resource via DELETE request.
        
        Args:
            endpoint (str): Full API endpoint URL
            
        Raises:
            HTTPError: If the request fails
        """
        headers = self.auth.get_headers()
        requests_delete(endpoint, headers=headers, rate_limit_delay=self.request_delay)
    
    def resource_exists(self, endpoint: str) -> bool:
        """
        Check if a resource exists.
        
        Args:
            endpoint (str): Full API endpoint URL
            
        Returns:
            bool: True if the resource exists, False otherwise
        """
        headers = self.auth.get_headers()
        
        try:
            requests_get(endpoint, headers=headers, rate_limit_delay=self.request_delay)
            return True
        except HTTPError as e:
            if e.response.status_code == 404:
                return False
            raise
