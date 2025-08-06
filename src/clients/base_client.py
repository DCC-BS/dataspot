from typing import Dict, Any, List
import logging
import json

import config
from src.dataspot_auth import DataspotAuth
from src.common import requests_get, requests_delete, requests_post, requests_put, requests_patch
from src.clients.helpers import url_join

from requests import HTTPError

class BaseDataspotClient:
    """Base class for Dataspot API clients implementing common functionality.
    
    Status Handling Philosophy:
    --------------------------
    1. New assets: By default, assets are created with status="WORKING" (DRAFT group).
       This can be overridden by passing a different status to creation methods.
       
    2. Updates: By default, updates will set status back to "WORKING" to allow for review.
       To preserve the current status of an asset during update, pass status=None.
       
    3. Bulk operations: Status handling follows the same principles, with the status
       parameter applied to all assets in the operation.
    """

    def __init__(self, base_url: str, database_name: str, scheme_name: str, scheme_name_short: str, 
                 ods_imports_collection_name: str = None, ods_imports_collection_path: List[str] = None):
        """
        Initialize the DataspotClient with the necessary credentials and configurations.
        
        Args:
            base_url: The base URL of the Dataspot API
            database_name: The name of the database
            scheme_name: The name of the scheme
            scheme_name_short: The short name of the scheme
            ods_imports_collection_name: Optional. The name of the default imports collection
            ods_imports_collection_path: Optional. The path of the default imports collection
        """
        self.auth = DataspotAuth()

        self.base_url = base_url
        self.database_name = database_name
        self.scheme_name = scheme_name
        self.scheme_name_short = scheme_name_short
        self.ods_imports_collection_name = ods_imports_collection_name
        self.ods_imports_collection_path = ods_imports_collection_path
        
        # Initialize cache for ODS imports collection
        self._ods_imports_collection = None
        
        # If ods_imports_collection_name is provided, initialize the collection
        if self.ods_imports_collection_name:
            self._ods_imports_collection = self.ensure_ods_imports_collection_exists()

    def execute_query_api(self, sql_query) -> List[Dict[str, Any]]:
        """
        Execute a query using the Dataspot Query API and return JSON results.

        Args:
            sql_query (str): The SQL query to execute

        Returns:
            dict: The query results
        """
        logging.debug("Connecting to Dataspot Query API...")

        # Prepare request data
        query_data = {
            "sql": sql_query
        }

        # API endpoint
        # TODO: Remove base_url and database_name as arguments of the constructor! They are always config.*
        #  then remove the following three lines.
        import config
        base_url = config.base_url
        database = config.database_name
        endpoint = f"{base_url}/api/{database}/queries/download?format=JSON"

        logging.debug(f"Sending query to endpoint: {endpoint}")

        response = requests_put(
            url=endpoint,
            json=query_data,
            headers=self.auth.get_headers()
        )

        return response.json()

    def get_all_assets_from_scheme(self, filter_function=None) -> List[Dict[str, Any]]:
        """
        Download all assets from a scheme using the Download API with optional filtering.
                                        
        Args:
            filter_function: Optional function that takes an asset and returns True if it should be included.
                **IMPORTANT:** Note that the filter is applied on flat asset, i.e. no customProperties field is present!

        Returns:
            List[Dict[str, Any]]: List of assets from the scheme (filtered if filter_function provided)
                
        Raises:
            HTTPError: If API requests fail
            ValueError: If the response format is unexpected or invalid
        """
        logging.info(f"Downloading assets from {self.scheme_name_short} scheme for mapping update")

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
            if filter_function:
                filtered_assets = [asset for asset in assets if filter_function(asset)]
                logging.info(f"Downloaded {len(assets)} assets from scheme '{self.scheme_name}', filtered to {len(filtered_assets)}")
                return filtered_assets
            else:
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

    def _create_asset(self, endpoint: str, data: Dict[str, Any], status: str = "WORKING") -> Dict[str, Any]:
        """
        Create a new asset via POST request.

        The provided data dictionary must contain a '_type' key specifying the asset type (e.g., 'Dataset', 'Collection').

        Args:
            endpoint (str): API endpoint path (will be joined with base_url)
            data (Dict[str, Any]): JSON data for the asset, must include '_type'
            status (str): Status to set on the asset. Defaults to "WORKING" (DRAFT group).
                         Set to None to use the default status for the asset type.

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
            
        # Set status if specified (None means use default status for the asset type)
        if status is not None:
            data_to_send['status'] = status

        response = requests_post(full_url, headers=headers, json=data_to_send)
        return response.json()

    def _update_asset(self, endpoint: str, data: Dict[str, Any], replace: bool = False, status: str = "WORKING") -> Dict[str, Any]:
        """
        Update an existing asset via PUT or PATCH request.

        The provided data dictionary must contain a '_type' key specifying the asset type.

        Args:
            endpoint (str): API endpoint path (will be joined with base_url)
            data (Dict[str, Any]): JSON data for the asset, must include '_type'
            replace (bool): Whether to completely replace (PUT) or partially update (PATCH)
            status (str): Status to set on the asset. Defaults to "WORKING" (DRAFT group).
                         Set to None to not modify the asset's status.

        Returns:
            Dict[str, Any]: JSON response from the API

        Raises:
            HTTPError: If the request fails
            ValueError: If the asset does not exist when using replace=True, or if data is missing '_type' key

        Notes:
            - By default, assets will have their status set to 'WORKING' (DRAFT group).
            - Pass status=None to preserve the asset's current status.
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

        # Set status if specified (None means don't modify status)
        if status is not None:
            data_to_send['status'] = status

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

    def set_asset_status(self, endpoint: str, status: str) -> Dict[str, Any]:
        """
        Set the status of an asset without modifying any other properties.
        
        This method should be used carefully to avoid automatically publishing 
        assets that shouldn't be public.

        Args:
            endpoint (str): API endpoint path (will be joined with base_url)
            status (str): The status to set (e.g., "WORKING", "PUBLISHED", "REVIEWDCC2")

        Returns:
            Dict[str, Any]: JSON response from the API

        Raises:
            HTTPError: If the request fails
            ValueError: If the asset does not exist
        """
        headers = self.auth.get_headers()
        full_url = url_join(self.base_url, endpoint)

        # First, get the current asset to determine its type
        current_asset = self._get_asset(endpoint)
        if current_asset is None:
            raise ValueError(f"Cannot set status for asset at {endpoint}: asset does not exist")

        # Prepare minimal update data with only the status change
        update_data = {
            "_type": current_asset.get("_type", "Unknown"),  # Required for PATCH
            "status": status
        }

        logging.info(f"Setting status of asset at {endpoint} to '{status}'")

        # Use PATCH to update only the status property
        response = requests_patch(full_url, headers=headers, json=update_data)
        response.raise_for_status()
        
        return response.json()

    def _delete_asset(self, endpoint: str, force_delete: bool = True) -> None:
        """
        Permanently deletes an asset via DELETE request.
        
        Args:
            endpoint (str): API endpoint path (will be joined with base_url)
            force_delete (bool, optional): Legacy parameter, kept for backward compatibility.
                                           Set to False to use _mark_asset_for_deletion instead.
                                           Defaults to True.

        Raises:
            HTTPError: If the request fails
            ValueError: If the asset does not exist or cannot be accessed
        """
        if not force_delete:
            # Redirect to marking method if force_delete is False
            return self._mark_asset_for_deletion(endpoint)
            
        headers = self.auth.get_headers()
        full_url = url_join(self.base_url, endpoint)
        
        # Completely delete the asset using DELETE request
        logging.info(f"Permanently deleting asset at {endpoint}")
        requests_delete(full_url, headers=headers)
    
    def _mark_asset_for_deletion(self, endpoint: str) -> None:
        """
        Marks an asset for deletion review by changing its status to "REVIEWDCC2" (Lösch-Prüfung DCC) via PATCH request.
        This allows for a review process before actual deletion.
        
        Args:
            endpoint (str): API endpoint path (will be joined with base_url)
            
        Raises:
            HTTPError: If the request fails
            ValueError: If the asset does not exist or cannot be accessed
        """
        headers = self.auth.get_headers()
        full_url = url_join(self.base_url, endpoint)
        
        # First, check if the asset exists
        current_asset = self._get_asset(endpoint)
        if current_asset is None:
            logging.warning(f"Cannot mark asset for review at {endpoint}: asset does not exist")
            return  # Early return if asset doesn't exist
        
        # Prepare update data with only the status field to change
        update_data = {
            "_type": current_asset.get("_type", "Unknown"),  # Required for PATCH
            "status": "REVIEWDCC2"
        }
        
        # Mark the asset for review by changing its status
        logging.info(f"Marking asset at {endpoint} for review (REVIEWDCC2)")
        
        # Use PATCH to update only the status property
        response = requests_patch(full_url, headers=headers, json=update_data)
        response.raise_for_status()

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
        # Return cached result if available
        if self._ods_imports_collection:
            logging.debug("Using cached ODS-Imports collection from initialization")
            return self._ods_imports_collection

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
                self._ods_imports_collection = existing_collection
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
                self._ods_imports_collection = response_json
                return response_json

        except HTTPError as create_error:
            logging.error(f"Failed to create ODS-Imports collection: {str(create_error)}")
            raise

    def bulk_create_or_update_assets(self, scheme_name: str, data: List[Dict[str, Any]],
                                     operation: str = "ADD", dry_run: bool = False, 
                                     status: str = "WORKING") -> Dict[str, Any]:
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
            status (str, optional): Status to set on all assets in the upload. Defaults to "WORKING" (DRAFT group).
                                   Set to None to preserve existing statuses or use asset type defaults.

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
            
            # Set status if specified (None means preserve existing or use defaults)
            if status is not None:
                item_copy['status'] = status
                
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

    # This method exists to avoid a pycharm warning.
    #  I don't really want to add an entire interface for just this case
    def sync_org_units(self, all_organizations: dict, status: str = "WORKING"):
        raise NotImplementedError("Error: sync_org_units should never be called in the BaseClient! If you see this, then this is a bug.")

    def get_org_units_by_staatskalender_ids(self, staatskalender_ids: List[str]) -> Dict[str, Dict[str, Any]]:
        """
        Fetch organization units by their staatskalender_id values.
        
        Args:
            staatskalender_ids: List of staatskalender_id values to fetch
            
        Returns:
            Dict mapping staatskalender_id to the full unit data, including UUID
        """
        if not staatskalender_ids:
            return {}
            
        # Get all org units with the appropriate filter function
        org_filter = lambda asset: (
            asset.get('_type') == 'Collection' and 
            asset.get('stereotype') == 'Organisationseinheit' and
            asset.get('id_im_staatskalender') is not None
        )
        
        units = self.get_all_assets_from_scheme(org_filter)
        
        # Build a lookup table mapping staatskalender_id to unit data
        result = {}
        for unit in units:
            # Convert to string for consistent comparison
            unit_id = str(unit.get('id_im_staatskalender'))
            if unit_id in staatskalender_ids:
                result[unit_id] = unit
                
        return result
        
    def ensure_person_exists(self, first_name: str, last_name: str) -> (str, bool):
        """
        Ensures that a person exists in the Dataspot database. If the person doesn't exist,
        creates them using just the first and last name.
        
        Args:
            first_name: First name of the person
            last_name: Last name of the person
            
        Returns:
            str: The uuid of the existing or newly created person
            bool: Indicates whether the person was newly created. False if person already existed
            
        Raises:
            HTTPError: If API requests fail
        """
        logging.info(f"Ensuring person exists: {first_name} {last_name}")
        
        # Get all persons from the database
        persons_endpoint = f"/rest/{self.database_name}/persons/"
        response = requests_get(
            url_join(self.base_url, persons_endpoint),
            headers=self.auth.get_headers()
        )
        response.raise_for_status()
        
        # Check response structure - could be a list or embedded in _embedded.persons
        response_json = response.json()
        if '_embedded' in response_json and 'persons' in response_json['_embedded']:
            persons = response_json['_embedded']['persons']
        elif isinstance(response_json, list):
            persons = response_json
        else:
            logging.warning(f"Unexpected response format from persons endpoint: {response_json}")
            persons = []
        
        # Look for existing person by name
        for person in persons:
            # Match by first and last name
            if person.get('familyName') == last_name and person.get('givenName') == first_name:
                logging.info(f"Found existing person: {person.get('familyName')} {person.get('givenName')} (id: {person.get('id')})")
                person_uuid = person['id']
                newly_created = False
                return person_uuid, newly_created
        
        # Person doesn't exist, create it
        logging.info(f"Person not found, creating new: {first_name} {last_name}")

        # Determine Data Excellence UUID
        dx_response = requests_get(f"{self.base_url}/rest/{self.database_name}/organizations/{config.organizations_name}",
                                   headers=self.auth.get_headers())

        dx_response.raise_for_status()
        dx_response_json = dx_response.json()

        dx_organization_uuid = dx_response_json['id']

        # Prepare person data
        person_data = {
            "_type": "Person",
            "familyName": last_name,
            "givenName": first_name,
            "agentOf": dx_organization_uuid
        }
            
        # Create the person
        create_endpoint = f"/rest/{self.database_name}/persons"

        new_person = self._create_asset(
            endpoint=create_endpoint,
            data=person_data
        )

        new_person_uuid = new_person['id']
        
        logging.info(f"Created new person: {first_name} {last_name} with ID: {new_person_uuid}")

        newly_created = True
        return new_person_uuid, newly_created

    def ensure_user_exists(self, email: str, person_uuid: str, access_level: str) -> (str, bool):
        """
        Ensures that a user exists in the Dataspot database. If the user doesn't exist,
        creates them using the email and first/last name.
        
        Args:
            email: Email address of the user (used as loginId)
            person_uuid: UUID of the person. Used for the isPerson field
            access_level: Access level for the user if created (READ_ONLY, EDITOR, ADMINISTRATOR)
            
        Returns:
            str: The uuid of the existing or newly created user
            bool: Indicates whether the user was newly created. False if user already existed
            
        Raises:
            HTTPError: If API requests fail
        """
        logging.info(f"Ensuring user exists: {email}")
        
        # Get all users from the database via the tenants endpoint
        users_endpoint = f"/api/{self.database_name}/tenants/Mandant/download?format=JSON"
        response = requests_get(
            url_join(self.base_url, users_endpoint),
            headers=self.auth.get_headers()
        )
        response.raise_for_status()
        
        # Parse response
        users = response.json()
        
        # Look for existing user by email
        for user in users:
            if user.get('loginId') == email:
                logging.info(f"Found existing user: {user.get('loginId')} (id: {user.get('id')})")
                newly_created = False
                return user.get('id'), newly_created
        
        # User doesn't exist, create it
        logging.info(f"User not found, creating new: {email}")
        
        # Prepare user data
        user_data = {
            "_type": "User",
            "loginId": email,
            "accessLevel": access_level,
            "isPerson": person_uuid
        }
            
        # Create the user via API
        users_create_url = f"{self.base_url}/rest/{self.database_name}/users"

        response = requests_post(
            url=users_create_url,
            json=user_data,
            headers=self.auth.get_headers()
        )
        response.raise_for_status()
        
        new_user = response.json()
        new_user_uuid = new_user.get('id')
        
        if not new_user_uuid:
            raise ValueError(f"Failed to create user: {email}. Response did not include user ID.")
        
        logging.info(f"Created new user: {email} with ID: {new_user_uuid}")
        
        newly_created = True
        return new_user_uuid, newly_created

    def get_user_by_email(self, email: str) -> Dict[str, Any]:
        """
        Get user details by email address.

        Args:
            email: Email address of the user to find

        Returns:
            Dict[str, Any]: User details if found, None otherwise
        """
        users_endpoint = f"/api/{self.database_name}/tenants/Mandant/download?format=JSON"
        response = requests_get(
            url_join(self.base_url, users_endpoint),
            headers=self.auth.get_headers()
        )

        if response.status_code != 200:
            logging.error(f"Could not fetch users data from Dataspot. Status code: {response.status_code}")
            return None

        users = response.json()

        # Find user with matching email
        for user in users:
            if user.get('loginId') == email:
                return user

        return None
