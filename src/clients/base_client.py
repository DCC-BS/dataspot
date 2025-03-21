from typing import Dict, Any
from abc import ABC, abstractmethod

from src import config
from src.dataspot_auth import DataspotAuth
from src.common import requests_get, requests_delete, requests_post, requests_put, requests_patch
from src.clients.helpers import url_join

from requests import HTTPError

# TODO (large language model): If possible, remove rate limiting from this class, and handle that in the common module.
# TODO (Renato): Add at least one @abstractmethod to properly enforce this class as abstract and prevent direct instantiation.
class BaseDataspotClient(ABC):
    """Base class for Dataspot API clients with common functionality."""

    def __init__(self, request_delay=1.0):
        """
        Initialize the DataspotClient with the necessary credentials and configurations.

        Args:
            request_delay (float, optional): The delay between API requests in seconds. Default is 1.0 second.
                                            This helps prevent overloading the server with too many requests.
        """
        self.auth = DataspotAuth()
        self.request_delay = request_delay

        # Load configuration from config.py
        self.base_url = config.base_url
        self.database_name = config.database_name
        self.ods_imports_collection_name = config.ods_imports_collection_name

    # TODO (large language model): Correctly use the _type parameter.
    def create_resource(self, _type: str, endpoint: str, data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Create a new resource via POST request.
        
        Args:
            _type (str): The type of resource to create (e.g. 'Dataset', 'Collection')
            endpoint (str): API endpoint path (will be joined with base_url)
            data (Dict[str, Any]): JSON data for the resource
            
        Returns:
            Dict[str, Any]: JSON response from the API
            
        Raises:
            HTTPError: If the request fails
        """
        headers = self.auth.get_headers()
        full_url = url_join(self.base_url, endpoint)
        response = requests_post(full_url, headers=headers, json=data, rate_limit_delay=self.request_delay)
        return response.json()
    
    def update_resource(self, endpoint: str, data: Dict[str, Any], replace: bool = False) -> Dict[str, Any]:
        """
        Update an existing resource via PUT or PATCH request.
        
        Args:
            endpoint (str): API endpoint path (will be joined with base_url)
            data (Dict[str, Any]): JSON data for the resource
            replace (bool): Whether to completely replace (PUT) or partially update (PATCH)
            
        Returns:
            Dict[str, Any]: JSON response from the API
            
        Raises:
            HTTPError: If the request fails
        """
        headers = self.auth.get_headers()
        full_url = url_join(self.base_url, endpoint)
        
        if replace:
            # Use PUT to completely replace the resource
            response = requests_put(full_url, headers=headers, json=data, rate_limit_delay=self.request_delay)
        else:
            # Use PATCH to update only the specified properties
            response = requests_patch(full_url, headers=headers, json=data, rate_limit_delay=self.request_delay)
            
        return response.json()
    
    def delete_resource(self, endpoint: str) -> None:
        """
        Delete a resource via DELETE request.
        
        Args:
            endpoint (str): API endpoint path (will be joined with base_url)
            
        Raises:
            HTTPError: If the request fails
        """
        headers = self.auth.get_headers()
        full_url = url_join(self.base_url, endpoint)
        requests_delete(full_url, headers=headers, rate_limit_delay=self.request_delay)
    
    def get_resource_if_exists(self, endpoint: str) -> Dict[str, Any] | None:
        """
        Get a resource if it exists, return None if it doesn't.
        
        Args:
            endpoint (str): API endpoint path (will be joined with base_url)
            
        Returns:
            Dict[str, Any] | None: The resource data (converted to json) if it exists, None if it doesn't
            
        Raises:
            HTTPError: If API requests fail with status codes other than 404
        """
        headers = self.auth.get_headers()
        full_url = url_join(self.base_url, endpoint)
        
        try:
            response = requests_get(full_url, headers=headers, rate_limit_delay=self.request_delay)
            return response.json()
        except HTTPError as e:
            if e.response.status_code == 404:
                return None
            raise
