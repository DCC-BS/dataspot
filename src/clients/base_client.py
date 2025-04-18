from typing import Dict, Any, List
from abc import ABC, abstractmethod
import logging
import json

from src import config
from src.dataspot_auth import DataspotAuth
from src.common import requests_get, requests_delete, requests_post, requests_put, requests_patch
from src.clients.helpers import url_join

from requests import HTTPError

class BaseDataspotClient(ABC):
    """Base class for Dataspot API clients with common functionality."""

    def __init__(self):
        """
        Initialize the DataspotClient with the necessary credentials and configurations.
        """
        self.auth = DataspotAuth()

        # Load configuration from config.py
        self.base_url = config.base_url
        self.database_name = config.database_name
        self.ods_imports_collection_name = config.ods_imports_collection_name

    @abstractmethod
    def require_scheme_exists(self) -> str:
        """
        Verify that the client's scheme exists and return its href.
        
        This method must be implemented by subclasses to ensure that the scheme
        they're intended to work with actually exists in the Dataspot system.
        
        Returns:
            str: The href of the scheme
            
        Raises:
            ValueError: If the scheme doesn't exist
        """
        pass

    def create_resource(self, endpoint: str, data: Dict[str, Any], _type: str = "Asset") -> Dict[str, Any]:
        """
        Create a new resource via POST request.
        
        Args:
            endpoint (str): API endpoint path (will be joined with base_url)
            data (Dict[str, Any]): JSON data for the resource
            _type (str, optional): The type of resource to create (e.g. 'Dataset', 'Collection'). Defaults to "Asset".
                                  This value will override any existing '_type' in data
            
        Returns:
            Dict[str, Any]: JSON response from the API
            
        Raises:
            HTTPError: If the request fails
        """
        headers = self.auth.get_headers()
        full_url = url_join(self.base_url, endpoint)
        
        # Clone the data to avoid modifying the original
        data_to_send = dict(data)
        
        # Add or override _type
        data_to_send["_type"] = _type
        
        response = requests_post(full_url, headers=headers, json=data_to_send)
        return response.json()

    def bulk_create_or_update_resources(self, scheme_name: str, data: List[Dict[str, Any]], _type: str = "Asset",
                                        operation: str = "ADD", dry_run: bool = False) -> Dict[str, Any]:
        """
        Create or update multiple resources in bulk via the upload API.
        
        Args:
            scheme_name (str): Name of the scheme to upload to (e.g. 'Datennutzungskatalog')
            data (List[Dict[str, Any]]): List of JSON data for resources to create/update
            _type (str, optional): The type of resources to create (e.g. 'Dataset', 'Collection'). Defaults to "Asset".
                                  This value will override any existing '_type' in each data item
            operation (str, optional): Upload operation mode. Defaults to "ADD".
                                      "ADD": Add or update only. Existing items not in the upload remain unchanged.
                                      "REPLACE": Reconcile elements. Items not in the upload are considered obsolete.
                                      "FULL_LOAD": Reconcile model. Completely replaces with the uploaded data.
            dry_run (bool, optional): Whether to perform a test run without changing data. Defaults to False.
            
        Returns:
            Dict[str, Any]: JSON response from the API
            
        Raises:
            HTTPError: If the request fails
            ValueError: If operation parameter is invalid
        """
        # Validate operation parameter
        valid_operations = ["ADD", "REPLACE", "FULL_LOAD"]
        if operation not in valid_operations:
            raise ValueError(f"Invalid operation: {operation}. Must be one of {valid_operations}")
        
        # Clone the data to avoid modifying the original and add _type to each item
        data_to_send = []
        for item in data:
            item_copy = dict(item)
            item_copy["_type"] = _type
            data_to_send.append(item_copy)
        
        # Create upload endpoint directly with scheme name
        # The API endpoint is always "/api/<database>/schemes/<scheme_name>/upload"
        upload_path = f"/api/{self.database_name}/schemes/{scheme_name}/upload"
        logging.debug(f"Creating upload path for scheme '{scheme_name}': {upload_path}")
        
        # Add query parameters
        query_params = []
        
        if operation != "ADD":
            query_params.append(f"operation={operation}")
        
        if dry_run:
            query_params.append("dryRun=true")
        
        if query_params:
            upload_path = f"{upload_path}?{'&'.join(query_params)}"
        
        # Prepare HTTP headers
        headers = self.auth.get_headers()
        
        # Remove Content-Type from headers as requests will set it automatically for multipart/form-data
        if 'Content-Type' in headers:
            del headers['Content-Type']
        
        # Create full URL
        full_url = url_join(self.base_url, upload_path)
        logging.debug(f"Upload endpoint URL: {full_url}")
        
        # Convert data to JSON string
        json_data = json.dumps(data_to_send, ensure_ascii=False)
        
        # Create a requests-compatible files dictionary that includes the JSON data from memory
        # This preserves the multipart/form-data format expected by the API
        files = {
            'import.json': ('import.json', json_data, 'application/json')
        }
        
        logging.debug(f"Sending data as multipart/form-data with import.json field")
        
        response = requests_put(
            full_url,
            headers=headers,
            files=files
        )
        
        # Raise HTTPError for bad responses
        response.raise_for_status()
        
        # Try to parse response as JSON
        try:
            return response.json()
        except json.JSONDecodeError:
            # If response is not JSON, return the text content
            logging.warning(f"Response was not valid JSON. Content: {response.text[:1000]}...")
            return {"response_text": response.text}
    
    def update_resource(self, endpoint: str, data: Dict[str, Any], replace: bool = False, _type: str = "Asset") -> Dict[str, Any]:
        """
        Update an existing resource via PUT or PATCH request.
        
        Args:
            endpoint (str): API endpoint path (will be joined with base_url)
            data (Dict[str, Any]): JSON data for the resource
            replace (bool): Whether to completely replace (PUT) or partially update (PATCH)
            _type (str, optional): The type of resource to update (e.g. 'Dataset', 'Collection'). Defaults to "Asset".
                                  This value will override any existing '_type' in data
            
        Returns:
            Dict[str, Any]: JSON response from the API
            
        Raises:
            HTTPError: If the request fails
            ValueError: If the resource does not exist when using replace=True
            
        Notes:
            - All resources will have their status set to 'WORKING' regardless of their previous status.
            - When using replace=True for Datasets, the method will preserve the dataset's location
              (inCollection field).
        """
        headers = self.auth.get_headers()
        full_url = url_join(self.base_url, endpoint)
        
        # Clone the data to avoid modifying the original
        data_to_send = dict(data)
        
        data_to_send['status'] = 'WORKING'
        
        # Add or override _type
        data_to_send["_type"] = _type
        
        if replace and _type == "Dataset":
            # When completely replacing a Dataset with PUT, we need to preserve its location
            current_resource = self.get_resource_if_exists(endpoint)
            if current_resource is None:
                raise ValueError(f"Cannot update resource at {endpoint}: Resource does not exist")
            
            if 'inCollection' in current_resource:
                data_to_send['inCollection'] = current_resource['inCollection']
        
        if replace:
            # Use PUT to completely replace the resource
            response = requests_put(full_url, headers=headers, json=data_to_send)
        else:
            # Use PATCH to update only the specified properties
            response = requests_patch(full_url, headers=headers, json=data_to_send)
            
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
        requests_delete(full_url, headers=headers)
    
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
            response = requests_get(full_url, headers=headers)
            return response.json()
        except HTTPError as e:
            if e.response.status_code in [404, 410]: # 404 for collections, 410 for datasets
                return None
            raise
