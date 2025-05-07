from typing import Dict, Any, List
import logging
import json

from src.dataspot_auth import DataspotAuth
from src.common import requests_get, requests_delete, requests_post, requests_put, requests_patch
from src.clients.helpers import url_join

from requests import HTTPError

class BaseDataspotClient():
    """Base class for Dataspot API clients implementing common functionality."""

    def __init__(self, base_url: str, database_name: str, scheme_name: str, scheme_name_short: str, ods_imports_collection_name: str, ods_imports_collection_path: str):
        """
        Initialize the DataspotClient with the necessary credentials and configurations.
        """
        self.auth = DataspotAuth()

        self.base_url = base_url
        self.database_name = database_name
        self.scheme_name = scheme_name
        self.scheme_name_short = scheme_name_short
        self.ods_imports_collection_name = ods_imports_collection_name
        self.ods_imports_collection_path = ods_imports_collection_path

    def get_all_assets_from_scheme(self) -> List[Dict[str, Any]]:
        """
        Download all assets from a scheme using the Download API.
                                        
        Returns:
            List[Dict[str, Any]]: List of assets from the scheme
            
        Raises:
            HTTPError: If API requests fail
            ValueError: If the response format is unexpected or invalid
        """
        logging.info(f"Downloading datasets from {self.scheme_name_short} scheme for mapping update")

        # Use the download API to retrieve assets from the scheme
        download_path = f"/api/{self.database_name}/schemes/{self.scheme_name}/download?format=JSON"
        full_url = url_join(self.base_url, download_path)
        
        logging.debug(f"Downloading all assets from scheme '{self.scheme_name}' at: {full_url}")
        response = requests_get(full_url, headers=self.auth.get_headers())
        response.raise_for_status()
        
        # Parse the JSON response
        assets = response.json()
        
        # If we got a list directly, use it
        if isinstance(assets, list):
            logging.info(f"Downloaded {len(assets)} assets from scheme '{self.scheme_name}'")
            return assets
        else:
            # We might have received a job ID instead
            logging.error(f"Received unexpected response format from {full_url}. Expected a list of assets.")
            logging.debug(f"Response content: {assets}")
            raise ValueError(f"Unexpected response format from download API. Expected a list but got: {type(assets)}")

    def _get_asset(self, endpoint: str) -> Dict[str, Any] | None:
        """
        Get a asset if it exists, return None if it doesn't.

        Args:
            endpoint (str): API endpoint path (will be joined with base_url)

        Returns:
            Dict[str, Any] | None: The asset data (converted to json) if it exists, None if it doesn't

        Raises:
            HTTPError: If API requests fail with status codes other than 404
        """
        headers = self.auth.get_headers()
        full_url = url_join(self.base_url, endpoint)

        try:
            # Pass silent_status_codes to prevent logging 404 and 410 errors
            response = requests_get(full_url, headers=headers, silent_status_codes=[404, 410])
            return response.json()
        except HTTPError as e:
            if e.response.status_code in [404, 410]:  # 404 for collections, 410 for datasets
                return None
            raise

    def _create_asset(self, endpoint: str, data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Create a new asset via POST request.

        The provided data dictionary must contain a '_type' key specifying the asset type (e.g., 'Dataset', 'Collection').

        Args:
            endpoint (str): API endpoint path (will be joined with base_url)
            data (Dict[str, Any]): JSON data for the asset, must include '_type'

        Returns:
            Dict[str, Any]: JSON response from the API

        Raises:
            HTTPError: If the request fails
            ValueError: If the data dictionary is missing the '_type' key
            TODO (Renato) IMPORTANT BUT NOT URGENT: What happens if the asset already exists? -> Should throw an error. Inspect the error it actually throws, and handle it accordingly.
        """
        headers = self.auth.get_headers()
        full_url = url_join(self.base_url, endpoint)

        # Clone the data to avoid modifying the original
        data_to_send = dict(data)

        # Validate that _type is present
        if "_type" not in data_to_send:
            raise ValueError("Input data for create_asset must contain a '_type' key.")

        response = requests_post(full_url, headers=headers, json=data_to_send)
        return response.json()

    def _update_asset(self, endpoint: str, data: Dict[str, Any], replace: bool = False) -> Dict[str, Any]:
        """
        Update an existing asset via PUT or PATCH request.

        The provided data dictionary must contain a '_type' key specifying the asset type.

        Args:
            endpoint (str): API endpoint path (will be joined with base_url)
            data (Dict[str, Any]): JSON data for the asset, must include '_type'
            replace (bool): Whether to completely replace (PUT) or partially update (PATCH)

        Returns:
            Dict[str, Any]: JSON response from the API

        Raises:
            HTTPError: If the request fails
            ValueError: If the asset does not exist when using replace=True, or if data is missing '_type' key

        Notes:
            - All assets will have their status set to 'WORKING' regardless of their previous status.
            - When using replace=True for Datasets, the method will preserve the dataset's location
              (inCollection field).
        """
        headers = self.auth.get_headers()
        full_url = url_join(self.base_url, endpoint)

        # Clone the data to avoid modifying the original
        data_to_send = dict(data)

        # Validate that _type is present
        if "_type" not in data_to_send:
            raise ValueError("Input data for update_asset must contain a '_type' key.")

        data_to_send['status'] = 'WORKING'

        # Get the type from the data itself
        asset_type = data_to_send.get("_type")

        if replace and asset_type == "Dataset":
            # When completely replacing a Dataset with PUT, we need to preserve its location
            current_asset = self._get_asset(endpoint)
            if current_asset is None:
                raise ValueError(f"Cannot update asset at {endpoint}: asset does not exist")

            if 'inCollection' in current_asset:
                data_to_send['inCollection'] = current_asset['inCollection']

        if replace:
            # Use PUT to completely replace the asset
            response = requests_put(full_url, headers=headers, json=data_to_send)
        else:
            # Use PATCH to update only the specified properties
            response = requests_patch(full_url, headers=headers, json=data_to_send)

        return response.json()

    def _delete_asset(self, endpoint: str) -> None:
        """
        Delete a asset via DELETE request.

        Args:
            endpoint (str): API endpoint path (will be joined with base_url)

        Raises:
            HTTPError: If the request fails
        """
        headers = self.auth.get_headers()
        full_url = url_join(self.base_url, endpoint)
        requests_delete(full_url, headers=headers)

    def require_scheme_exists(self) -> str:
        """
        Assert that the scheme exists and return its API endpoint. Throw an error if it doesn't.

        Returns:
            str: The API endpoint of the scheme (starting with /rest/...)

        Raises:
            ValueError: If the scheme doesn't exist
            HTTPError: If API requests fail
        """
        scheme_path = url_join('rest', self.database_name, 'schemes', self.scheme_name)
        scheme_response = self._get_asset(scheme_path)
        if not scheme_response:
            raise ValueError(f"Scheme '{self.scheme_name}' does not exist")
        return scheme_response['_links']['self']['href']

    def ensure_ods_imports_collection_exists(self) -> dict:
        """
        Ensures that the ODS-Imports collection exists within the scheme.

        The path is defined by self.ods_imports_collection_path, which is a list of folder names.
        For example, if self.ods_imports_collection_path is ['A', 'B', 'C'], the function:
        1. First checks if 'A/B/C' path already exists
        2. If the path doesn't exist, logs an error and throws an exception
        3. If the path exists, checks if ODS-Imports collection exists at that location
        4. Creates the ODS-Imports collection if it doesn't exist, or returns the existing one if it does

        Returns:
            dict: The JSON response containing information about the ODS-Imports collection

        Raises:
            ValueError: If the scheme does not exist or the configured path contains a '/' or the configured path doesn't exist
            HTTPError: If API requests fail
        """
        logging.info("Ensuring ODS-Imports collection exists")
        # Assert that the scheme exists.
        self.require_scheme_exists()

        # Validate that the path contains only strings
        for item in self.ods_imports_collection_path:
            if type(item) != str:
                logging.error(f"Path defined in config.py contains {item}, which is not a string.")
                raise ValueError(
                    f"Invalid path component in ods_imports_collection_path: {item}. All path components must be strings.")

        if self.ods_imports_collection_path:
            logging.debug(f"Using configured path for ODS-Imports: {'/'.join(self.ods_imports_collection_path)}")
        else:
            logging.debug("No specific path configured for ODS-Imports, using scheme root")

        # Check for special characters that would prevent using business keys
        has_special_chars = False
        for folder in self.ods_imports_collection_path:
            if '/' in folder:
                has_special_chars = True
                logging.warning(
                    f"Collection path contains forward slashes, which can't be used in business keys: {folder}")
                break

        # Check if the configured path exists
        if not self.ods_imports_collection_path:
            # No path specified, check directly under scheme
            parent_endpoint = url_join('rest', self.database_name, 'schemes', self.scheme_name)
            parent_response = self._get_asset(parent_endpoint)
            if not parent_response:
                error_msg = f"Scheme '{self.scheme_name}' does not exist"
                logging.error(error_msg)
                raise ValueError(error_msg)

            # Parent exists (scheme root), check if ODS-Imports exists
            ods_imports_endpoint = url_join(parent_endpoint, 'collections', self.ods_imports_collection_name,
                                            leading_slash=True)
            collections_endpoint = url_join(parent_endpoint, 'collections', leading_slash=True)
            existing_collection = self._get_asset(ods_imports_endpoint)

            # Check both existence and correct parent
            ods_imports_exists = False
            if existing_collection:
                # For root collections, parentId should match the scheme UUID
                if 'parentId' in existing_collection and existing_collection['parentId'] == parent_response['id']:
                    ods_imports_exists = True
                else:
                    logging.warning(
                        f"Found ODS-Imports collection but it's not under the expected parent. Will create new one.")

        elif has_special_chars:
            error_msg = ("Path contains special characters that prevent using business keys. Fix the path in config. "
                         "Using Collections that contain a slash is currently not supported in ODS-Imports path. "
                         "Implementing this would be time-consuming and likely introduce errors. "
                         "Also, I don't think this error will ever happen, so I will not fix it at the moment.")
            logging.error(error_msg)
            raise ValueError(error_msg)
        else:
            # Construct business key path to check if the full path exists
            # Format: /rest/{db}/schemes/{scheme}/collections/{col1}/collections/{col2}/...
            path_elements = ['rest', self.database_name, 'schemes', self.scheme_name]

            # Build up the path with 'collections' between each element
            for folder in self.ods_imports_collection_path:
                path_elements.append('collections')
                path_elements.append(folder)

            # Check if the parent path exists
            parent_path = url_join(*path_elements, leading_slash=True)
            parent_response = self._get_asset(parent_path)

            if not parent_response:
                # Parent path doesn't exist - throw error instead of creating it
                error_msg = (f"Configured path '{'/'.join(self.ods_imports_collection_path)}' not found. "
                             f"Please make sure the ods_imports_collection_path field in config.py is set correctly!")
                logging.error(error_msg)
                raise ValueError(error_msg)

            # Parent path exists, check if ODS-Imports exists under it
            collections_endpoint = url_join(parent_path, 'collections', leading_slash=True)

            # Create ODS-Imports endpoint for checking existence
            ods_imports_elements = path_elements.copy()
            ods_imports_elements.append('collections')
            ods_imports_elements.append(self.ods_imports_collection_name)
            ods_imports_endpoint = url_join(*ods_imports_elements, leading_slash=True)
            existing_collection = self._get_asset(ods_imports_endpoint)

            # Check both existence and correct parent
            ods_imports_exists = False
            if existing_collection:
                # Verify the collection is under the expected parent
                if 'parentId' in existing_collection and existing_collection['parentId'] == parent_response['id']:
                    ods_imports_exists = True
                else:
                    logging.warning(
                        f"Found ODS-Imports collection but it's not under the expected parent. Will create new one.")

        try:
            # Return existing or create new
            if ods_imports_exists:
                logging.debug(f"ODS-Imports collection already exists under the correct parent, using it as is")
                path_str = "/".join(self.ods_imports_collection_path) if self.ods_imports_collection_path else "scheme root"
                logging.info(f"ODS-Imports collection found at: {path_str}")
                return existing_collection
            else:
                logging.debug(f"ODS-Imports collection does not exist under the correct parent, creating it")
                collection_data = {
                    "label": self.ods_imports_collection_name,
                    "_type": "Collection"
                }
                response_json = self._create_asset(
                    endpoint=collections_endpoint,
                    data=collection_data
                )
                path_str = "/".join(self.ods_imports_collection_path) if self.ods_imports_collection_path else "scheme root"
                logging.info(f"Created ODS-Imports collection at: {path_str}")
                return response_json

        except HTTPError as create_error:
            logging.error(f"Failed to create ODS-Imports collection: {str(create_error)}")
            raise

    def bulk_create_or_update_assets(self, scheme_name: str, data: List[Dict[str, Any]],
                                     operation: str = "ADD", dry_run: bool = False) -> Dict[str, Any]:
        """
        Create or update multiple assets in bulk via the upload API.

        Each dictionary in the data list must contain a '_type' key specifying the asset type.

        Args:
            scheme_name (str): Name of the scheme to upload to (e.g. 'Datennutzungskatalog')
            data (List[Dict[str, Any]]): List of JSON data for assets to create/update. Each dict must include '_type'.
            operation (str, optional): Upload operation mode. Defaults to "ADD".
                                      "ADD": Add or update only. Existing items not in the upload remain unchanged.
                                      "REPLACE": Reconcile elements. Items not in the upload are considered obsolete.
                                      "FULL_LOAD": Reconcile model. Completely replaces with the uploaded data.
            dry_run (bool, optional): Whether to perform a test run without changing data. Defaults to False.

        Returns:
            Dict[str, Any]: JSON response from the API

        Raises:
            HTTPError: If the request fails
            ValueError: If operation parameter is invalid or any data item is missing the '_type' key
        """
        # Validate operation parameter
        valid_operations = ["ADD", "REPLACE", "FULL_LOAD"]
        if operation not in valid_operations:
            raise ValueError(f"Invalid operation: {operation}. Must be one of {valid_operations}")

        # Clone the data to avoid modifying the original and validate _type
        data_to_send = []
        for i, item in enumerate(data):
            if "_type" not in item:
                raise ValueError(
                    f"Item at index {i} in data list for bulk_create_or_update_assets is missing the '_type' key.")
            item_copy = dict(item)
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
