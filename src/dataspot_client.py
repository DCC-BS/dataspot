import logging

from requests import HTTPError
from time import sleep

from src.dataspot_auth import DataspotAuth
from src.common import requests_get, requests_delete, requests_post, requests_put, requests_patch
from src.dataspot_uuid_cache import DataspotUUIDCache
import src.clients.helpers as client_helpers
import src.config as config
import json

from src.dataspot_dataset import Dataset

def url_join(*parts: str) -> str:
    """
    Join URL parts ensuring proper formatting with slashes.
    
    Args:
        *parts: URL parts to be joined.
        
    Returns:
        str: A properly formatted URL.
    """
    return client_helpers.url_join(*parts)

def generate_potential_staatskalender_url(path: str) -> str:
    """
    Generate a URL for the Basel Staatskalender based on an organization path string.
    
    This function transforms an organization path into a standardized URL format
    by applying these transformations:
    - Convert to lowercase
    - Replace spaces with hyphens
    - Convert German umlauts (ö→oe, ä→ae, ü→ue)
    - Remove all characters except letters, hyphens, and forward slashes
    - Replace double hyphens with single ones
    - Remove trailing slashes
    
    Args:
        path (str): The organization path string to transform
        
    Returns:
        str: A formatted URL pointing to the organization in the Basel Staatskalender
        
    Example:
        >>> generate_potential_staatskalender_url("Präsidialdepartement/Kantons- und Stadtentwicklung")
        "https://staatskalender.bs.ch/organization/praesidialdepartement/kantons-und-stadtentwicklung"
    """
    return client_helpers.generate_potential_staatskalender_url(path)

def escape_special_chars(name: str) -> str:
    '''
    Escape special characters in asset names for Dataspot API according to the business key rules.
    
    According to Dataspot documentation, special characters need to be properly escaped in business keys:
    
    1. If a name contains / or ., it should be enclosed in double quotes
       Example: INPUT/OUTPUT → "INPUT/OUTPUT"
       Example: dataspot. → "dataspot."
    
    2. If a name contains double quotes ("), each double quote should be doubled ("") and 
       the entire name should be enclosed in double quotes
       Example: 28" City Bike → "28"" City Bike"
       Example: Project "Zeus" → "Project ""Zeus"""
    
    Args:
        name (str): The name of the asset (dataset, organizational unit, etc.)
        
    Returns:
        str: The escaped name suitable for use in Dataspot API business keys
    '''
    
    return client_helpers.escape_special_chars(name)

class DataspotClient:
    """Client for interacting with the Dataspot API."""

    def __init__(self, request_delay=1.0, uuid_cache_path="dataspot_uuids.csv"):
        # TODO: Add also_save_to_csv_path argument that can be None or a path
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

        # Initialize UUID cache
        self.uuid_cache = DataspotUUIDCache(uuid_cache_path) if uuid_cache_path else None

    def teardown_dnk(self, delete_empty_collections: bool = False, ignore_status: bool = False) -> None:
        """
        Delete all OGD datasets from the DNK scheme and optionally remove empty collections.
        
        This method:
        1. Recursively traverses all collections in the DNK scheme
        2. Deletes only datasets with stereotype "OGD"
        3. Optionally removes collections that become empty after deleting their datasets
        4. Preserves the root DNK scheme even if empty
        5. Optionally ignores the status of the datasets and deletes them anyway
        
        Args:
            delete_empty_collections (bool): Whether to delete empty collections after removing datasets.
                                            Defaults to False.
            ignore_status (bool): Whether to ignore the status of the datasets and delete them anyway.
                                Defaults to False.
        
        Raises:
            requests.exceptions.RequestException: If the request fails
            json.JSONDecodeError: If the response is not valid JSON
        """
        relative_path = url_join('schemes', self.dnk_scheme_name)
        endpoint = url_join(self.base_url, 'rest', self.database_name, relative_path)
        headers = self.auth.get_headers()
        
        def delete_ogd_datasets_and_empty_collections(collection_url, is_root=False):
            """
            Recursively delete OGD datasets and optionally empty collections.
            
            Args:
                collection_url (str): URL of the collection to process
                is_root (bool): Whether this is the root DNK scheme (which should not be deleted)
                
            Returns:
                bool: True if the collection is empty after processing, False otherwise
            """
            # Get collection details
            response = requests_get(collection_url, headers=headers, rate_limit_delay=self.request_delay)
            collection_data = response.json()
            collection_uuid = collection_data['id']
            
            # Process datasets in this collection
            # For the root level, use 'schemes' instead of 'collections' in the URL
            if is_root:
                datasets_url = url_join(self.base_url, 'rest', self.database_name, 'schemes', collection_uuid, 'datasets')
            else:
                datasets_url = url_join(self.base_url, 'rest', self.database_name, 'collections', collection_uuid, 'datasets')
            
            datasets_response = requests_get(datasets_url, headers=headers, rate_limit_delay=self.request_delay)
            datasets_data = datasets_response.json()
            datasets = datasets_data.get('_embedded', {}).get('datasets', [])
            
            # Delete OGD datasets
            for dataset in datasets:
                dataset_url = dataset['_links']['self']['href']
                dataset_label = dataset.get('label', 'Unknown')
                
                # Check if dataset has stereotype "OGD"
                if dataset.get('stereotype') == "OGD":
                    if dataset.get('label') == 'Datensatz OGD':
                        logging.info(f"Ignoring OGD dataset: Datensatz OGD")
                        continue
                    logging.info(f"Deleting OGD dataset: {dataset_label}")
                    if ignore_status:
                        requests_patch(url_join(self.base_url, dataset_url), headers=headers, json={"_type": "Dataset", "status": "REVIEWDCC2"}, rate_limit_delay=self.request_delay)
                    requests_delete(url_join(self.base_url, dataset_url), headers=headers, rate_limit_delay=self.request_delay)
            
            # Process sub-collections
            # For the root level, use 'schemes' instead of 'collections' in the URL
            if is_root:
                collections_url = url_join(self.base_url, 'rest', self.database_name, 'schemes', collection_uuid, 'collections')
            else:
                collections_url = url_join(self.base_url, 'rest', self.database_name, 'collections', collection_uuid, 'collections')
            
            collections_response = requests_get(collections_url, headers=headers, rate_limit_delay=self.request_delay)
            collections_data = collections_response.json()
            sub_collections = collections_data.get('_embedded', {}).get('collections', [])
            
            # Process each sub-collection and track which ones become empty
            empty_subcollections = []
            for sub_collection in sub_collections:
                sub_collection_url = sub_collection['_links']['self']['href']
                sub_collection_label = sub_collection.get('label', 'Unknown')
                
                # Recursively process sub-collection (not root)
                is_empty = delete_ogd_datasets_and_empty_collections(url_join(self.base_url, sub_collection_url), is_root=False)
                
                if is_empty and delete_empty_collections:
                    empty_subcollections.append((sub_collection_url, sub_collection_label))
            
            # Delete empty sub-collections if delete_empty_collections is True
            if delete_empty_collections:
                for sub_url, sub_label in empty_subcollections:
                    logging.info(f"Deleting empty collection: {sub_label}")
                    requests_delete(url_join(self.base_url, sub_url), headers=headers, rate_limit_delay=self.request_delay)
            
            # Check if this collection is now empty
            # Refresh dataset and collection data after deletions
            datasets_response = requests_get(datasets_url, headers=headers, rate_limit_delay=self.request_delay)
            datasets_data = datasets_response.json()
            remaining_datasets = datasets_data.get('_embedded', {}).get('datasets', [])
            
            collections_response = requests_get(collections_url, headers=headers, rate_limit_delay=self.request_delay)
            collections_data = collections_response.json()
            remaining_collections = collections_data.get('_embedded', {}).get('collections', [])
            
            # Return True if collection is empty (no datasets and no sub-collections)
            # But don't actually delete the root schema
            is_empty = len(remaining_datasets) == 0 and len(remaining_collections) == 0
            return is_empty and not is_root
        
        # Start the recursive deletion process from the root DNK scheme
        # Pass is_root=True to prevent deletion of the root schema
        delete_ogd_datasets_and_empty_collections(endpoint, is_root=True)
        if delete_empty_collections:
            logging.info("Finished cleaning up OGD datasets and empty collections from DNK")
        else:
            logging.info("Finished cleaning up OGD datasets from DNK")

    def ods_type_to_dataspot_uuid(self, ods_type: str) -> str:
        """
        Convert an ODS data type directly to its corresponding Dataspot UUID.

        Args:
            ods_type (str): The ODS data type

        Returns:
            str: The UUID of the corresponding Dataspot type

        Raises:
            HTTPError: If the type doesn't exist or other HTTP errors occur
            json.JSONDecodeError: If the response is not valid JSON
        """
        # Map ODS types to UML data types
        type_mapping = {
            'text': '/Datentypmodell/Zeichenkette',
            'int': '/Datentypmodell/Ganzzahl',
            'boolean': '/Datentypmodell/Wahrheitswert',
            'double': '/Datentypmodell/Dezimalzahl',
            'datetime': '/Datentypmodell/Zeitpunkt',
            'date': '/Datentypmodell/Datum',
            'geo_point_2d': '/Datentypmodell/geo_point_2d',
            'geo_shape': '/Datentypmodell/geo_shape',
            'file': '/Datentypmodell/Binärdaten',
            'json_blob': '/Datentypmodell/Zeichenkette',
            'identifier': '/Datentypmodell/Identifier'
        }

        # Map the ODS type to Dataspot type path
        dataspot_type = type_mapping.get(ods_type.lower(), 'UNKNOWN TYPE')

        # Check UUID cache first if enabled
        if self.uuid_cache:
            cached_uuid = self.uuid_cache.get_uuid('Datatype', dataspot_type)
            if cached_uuid:
                return cached_uuid

        # Split the path to get the type name
        parts = dataspot_type.split('/')
        type_name = parts[-1]  # Get the last part of the path

        # Use find_datatype_path to handle potential slashes in type names
        datatype_path = self.find_datatype_path(type_name)
        endpoint = url_join(self.base_url, datatype_path)
        headers = self.auth.get_headers()

        try:
            response = requests_get(endpoint, headers=headers)
            type_uuid = response.json()['id']

            # Cache the result if caching is enabled
            if self.uuid_cache and type_uuid:
                self.uuid_cache.add_or_update_asset('Datatype', dataspot_type, type_uuid, datatype_path)
                
            return type_uuid

        except HTTPError as e:
            logging.error(f"Failed to get UUID for type {dataspot_type}: {str(e)}")
            raise
        except json.JSONDecodeError as e:
            raise json.JSONDecodeError(
                f"Failed to decode JSON response from {endpoint}: {str(e)}",
                e.doc,
                e.pos
            )

    def teardown_tdm(self) -> None:
        """
        Delete all collections within 'Automatisch generierte ODS-Datenmodelle' from the TDM scheme,
        but keep the parent collection and scheme intact.

        Raises:
            requests.exceptions.RequestException: If the request fails
            json.JSONDecodeError: If the response is not valid JSON
        """
        
        # First, get the UUID of the target collection
        collection_path = url_join('rest', self.database_name, 'schemes', self.tdm_scheme_name, 'collections', self.ods_imports_collection_name)
        endpoint = url_join(self.base_url, collection_path)
        headers = self.auth.get_headers()
        
        try:
            response = requests_get(endpoint, headers=headers)
            collection_uuid = response.json()['id']
            logging.debug(f"Found collection UUID: {collection_uuid}")
        except HTTPError as e:
            if e.response.status_code == 404:
                logging.info(f"Collection '{self.tdm_scheme_name}/collections/{self.ods_imports_collection_name}' not found. Nothing to delete.")
                return
            raise e
        
        # Get all assets within the target collection
        assets_path = url_join('rest', self.database_name, 'collections', collection_uuid, 'assets')
        assets_endpoint = url_join(self.base_url, assets_path)
        
        response = requests_get(assets_endpoint, headers=headers)
        response_json = response.json()
        assets = response_json.get('_embedded', {}).get('assets', [])
        
        # Delete each asset
        for asset in assets:
            asset_url = asset['_links']['self']['href']
            asset_label = asset['label']
            logging.info(f"Deleting asset: {asset_url} - {asset_label}")
            requests_delete(url_join(self.base_url, asset_url), headers=headers)

    def tdm_create_or_update_dataobject(self, name: str, columns: list[dict] = None) -> None:
        """
        Create a new dataobject (also called asset) in the 'ODS-Imports' collection in the 'Technisches Datenmodell' in Dataspot or update an existing one.
        The attributes of this dataobject are determined by the provided columns.

        Args:
            name (str): The name of the dataobject to create or update.
            columns (list[dict]): List of column information (one column per attribute), each containing:
                - label: Human-readable label
                - name: Technical column name
                - type: Data type of the column

        Raises:
            HTTPError: If the collection doesn't exist or other HTTP errors occur
            json.JSONDecodeError: If the response is not valid JSON
        """
        headers = self.auth.get_headers()
        asset_exists = False
        asset_href = None
        asset_uuid = None
        collection_uuid = None

        # 1. Get collection UUID (from cache if possible)
        if self.uuid_cache:
            collection_uuid = self.uuid_cache.get_uuid('Collection', self.ods_imports_collection_name)
        
        if not collection_uuid:
            collection_path = url_join('rest', self.database_name, 'schemes', self.tdm_scheme_name, 'collections', self.ods_imports_collection_name)
            endpoint = url_join(self.base_url, collection_path)

            try:
                response = requests_get(endpoint, headers=headers, rate_limit_delay=self.request_delay)
                collection_uuid = response.json()['id']
                logging.debug(f"Found collection UUID: {collection_uuid}")
                
                # Cache the collection UUID if caching enabled
                if self.uuid_cache and collection_uuid:
                    self.uuid_cache.add_or_update_asset('Collection', self.ods_imports_collection_name, collection_uuid, collection_path)
            except HTTPError as e:
                if e.response.status_code == 404:
                    raise HTTPError(f"Collection '{self.tdm_scheme_name}/collections/{self.ods_imports_collection_name}' does not exist") from e
                raise e

        # 2. Check if asset already exists (from cache if possible)
        if self.uuid_cache:
            asset_uuid = self.uuid_cache.get_uuid('TDMDataobject', name)
            asset_href = self.uuid_cache.get_href('TDMDataobject', name)
            if asset_uuid and asset_href:
                asset_exists = True
                asset_url = url_join(self.base_url, asset_href)
                try:
                    response = requests_get(asset_url, headers=headers, rate_limit_delay=self.request_delay)
                    asset_href = response.json()['_links']['self']['href']
                    logging.info(f"Dataobject '{name}' found in cache. Proceeding to update...")
                except HTTPError:
                    # Cache is outdated, will search manually
                    asset_exists = False
                    asset_uuid = None
                    asset_href = None

        if not asset_exists:
            try:
                asset_url = url_join(self.base_url, self.find_tdm_dataobject_path(name))
                response = requests_get(asset_url, headers=headers, rate_limit_delay=self.request_delay)
                asset_exists = True
                asset_href = response.json()['_links']['self']['href']
                asset_uuid = response.json()['id']
                logging.info(f"Dataobject '{name}' already exists. Proceeding to update...")
                
                # Cache the asset UUID if caching enabled
                if self.uuid_cache and asset_uuid:
                    self.uuid_cache.add_or_update_asset('TDMDataobject', name, asset_uuid, asset_href)
            except HTTPError as e:
                if e.response.status_code != 404:
                    raise e
                logging.info(f"Dataobject '{name}' does not exist. Will create a new one...")
            except ValueError:
                logging.info(f"Dataobject '{name}' does not exist. Will create a new one...")

        # 3. Create or update asset
        if not asset_exists:
            # Prepare asset data for new creation
            asset_data = {
                "_type": "UmlClass",
                "label": name
            }

            # Create new asset
            assets_path = url_join('rest', self.database_name, 'collections', collection_uuid, 'assets')
            assets_endpoint = url_join(self.base_url, assets_path)
            try:
                response = requests_post(assets_endpoint, headers=headers, json=asset_data, rate_limit_delay=self.request_delay)
                logging.info(f"Asset '{name}' created successfully.")
                asset_href = response.json()['_links']['self']['href']
                asset_uuid = response.json()['id']
                
                # Cache the asset UUID if caching enabled
                if self.uuid_cache and asset_uuid:
                    self.uuid_cache.add_or_update_asset('TDMDataobject', name, asset_uuid, asset_href)
            except json.JSONDecodeError as e:
                raise json.JSONDecodeError(
                    f"Failed to decode JSON response from {assets_endpoint}: {str(e)}",
                    e.doc,
                    e.pos
                )
        
        # Process attributes if columns are provided
        if not columns:
            logging.info("No columns provided. Skipping attribute creation.")
            return
            
        # 4. Get all existing attributes in a single call
        existing_attributes = {}
        if asset_exists:
            attributes_endpoint = url_join(self.base_url, asset_href, 'attributes')
            try:
                response = requests_get(attributes_endpoint, headers=headers, rate_limit_delay=self.request_delay)
                attrs_data = response.json()
                if '_embedded' in attrs_data and 'attributes' in attrs_data['_embedded']:
                    for attr in attrs_data['_embedded']['attributes']:
                        existing_attributes[attr['label']] = attr
            except Exception as e:
                logging.warning(f"Failed to get existing attributes: {str(e)}")
        
        # 5. Prepare attributes to create and update
        attributes_to_create = []
        attributes_to_update = []
        
        for col in columns:
            attribute = {
                "_type": "UmlAttribute",
                "title": col['label'],
                "label": col['name'],
                "hasRange": self.ods_type_to_dataspot_uuid(col['type'])
            }
            
            if col['name'] in existing_attributes:
                # Add to update list
                attributes_to_update.append((existing_attributes[col['name']]['_links']['self']['href'], attribute))
            else:
                # Add to create list
                attributes_to_create.append(attribute)
        
        # 6. Process attribute updates
        for attr_href, attribute in attributes_to_update:
            attr_endpoint = url_join(self.base_url, attr_href)
            try:
                requests_patch(attr_endpoint, headers=headers, json=attribute, rate_limit_delay=self.request_delay)
                logging.info(f"Attribute '{attribute['label']}' updated successfully.")
            except Exception as e:
                logging.error(f"Failed to update attribute '{attribute['label']}': {str(e)}")
                raise
        
        # 7. Process attribute creations
        asset_attributes_endpoint = url_join(self.base_url, asset_href, 'attributes')
        for attribute in attributes_to_create:
            try:
                requests_post(asset_attributes_endpoint, headers=headers, json=attribute, rate_limit_delay=self.request_delay)
                logging.info(f"Attribute '{attribute['label']}' created successfully.")
            except Exception as e:
                logging.error(f"Failed to create attribute '{attribute['label']}': {str(e)}")
                raise

    def dnk_create_or_update_organizational_unit(self, name: str, parent_name: str = None, custom_properties: dict = None) -> None:
        """
        Create a new organizational unit in the 'Datennutzungskatalog' of Dataspot or update an existing one.
        This method replaces the previous separate methods for creating departments, dienststelle, and sammlungen,
        as all now use the same "Organisationseinheit" stereotype.

        Args:
            name (str): The name of the organizational unit.
            parent_name (str, optional): The name of the parent organizational unit. If None, creates a top-level unit.
            custom_properties (dict, optional): Custom properties to be added to the organizational unit.

        Returns:
            None: The method doesn't return anything.

        Raises:
            HTTPError: If the parent doesn't exist or other HTTP errors occur.
            json.JSONDecodeError: If the response is not valid JSON.
        """
        headers = self.auth.get_headers()

        # Create data for the organizational unit
        org_unit_data = {
            "_type": "Collection",
            "label": name,
            "stereotype": "Organisationseinheit"
        }

        # Add custom properties if provided
        if custom_properties:
            org_unit_data["customProperties"] = custom_properties

        # If no parent specified, create as top-level in DNK scheme
        if parent_name is None:
            relative_path = url_join('rest', self.database_name, 'schemes', self.dnk_scheme_name, 'collections')
            endpoint = url_join(self.base_url, relative_path)
            
            # Check if unit already exists at the top level
            try:
                url_to_check = url_join('rest', self.database_name, 'schemes', self.dnk_scheme_name, 'collections', name)
                check_endpoint = url_join(self.base_url, url_to_check)
                response = requests_get(check_endpoint, headers=headers)
                
                # If unit exists and we have caching enabled, cache its UUID
                if self.uuid_cache:
                    unit_uuid = response.json()['id']
                    if unit_uuid:
                        self.uuid_cache.add_or_update_asset('OrganizationalUnit', name, unit_uuid, url_to_check)
                        
                logging.info(f"Organizational unit '{name}' already exists at top level. Skip creation...")
                return
            except HTTPError as e:
                if e.response.status_code != 404:
                    raise e
        else:
            # If parent specified, first get the parent's UUID
            parent_path = url_join('rest', self.database_name, 'schemes', self.dnk_scheme_name, 'collections', parent_name)
            parent_endpoint = url_join(self.base_url, parent_path)
            
            try:
                response = requests_get(parent_endpoint, headers=headers)
                parent_uuid = response.json()['id']
                logging.debug(f"Retrieved parent UUID: {parent_uuid}")
                
                # Cache parent UUID if caching enabled
                if self.uuid_cache and parent_uuid:
                    self.uuid_cache.add_or_update_asset('OrganizationalUnit', parent_name, parent_uuid, parent_path)
            except HTTPError as e:
                if e.response.status_code == 404:
                    logging.error(f"Parent organizational unit '{parent_name}' does not exist")
                    raise HTTPError(f"Parent organizational unit '{parent_name}' does not exist") from e
                raise e
                
            # Set the endpoint for creating under the parent
            relative_path = url_join('rest', self.database_name, 'collections', parent_uuid, 'collections')
            endpoint = url_join(self.base_url, relative_path)
            
            # Check if unit already exists under the parent
            try:
                url_to_check = url_join(endpoint, name)
                response = requests_get(url_to_check, headers=headers)
                
                # If unit exists and we have caching enabled, cache its UUID
                if self.uuid_cache:
                    unit_uuid = response.json()['id']
                    if unit_uuid:
                        self.uuid_cache.add_or_update_asset('OrganizationalUnit', name, unit_uuid, url_to_check)
                        
                logging.info(f"Organizational unit '{name}' already exists under '{parent_name}'. Skip creation...")
                return
            except HTTPError as e:
                if e.response.status_code != 404:
                    raise e

        # Create the organizational unit
        try:
            response = requests_post(endpoint, headers=headers, json=org_unit_data, rate_limit_delay=self.request_delay)
            
            # Cache the newly created unit UUID if caching enabled
            if self.uuid_cache:
                try:
                    unit_uuid = response.json()['id']
                    if unit_uuid:
                        # Construct proper path for the unit
                        if parent_name is None:
                            unit_path = url_join('rest', self.database_name, 'schemes', self.dnk_scheme_name, 'collections', name)
                        else:
                            unit_path = response.json()['_links']['self']['href']
                        
                        self.uuid_cache.add_or_update_asset('OrganizationalUnit', name, unit_uuid, unit_path)
                except Exception as e:
                    logging.warning(f"Failed to cache organizational unit UUID: {str(e)}")
            
            if parent_name:
                logging.info(f"Organizational unit '{name}' created under '{parent_name}'.")
            else:
                logging.info(f"Top-level organizational unit '{name}' created.")
            return
        except json.JSONDecodeError as e:
            raise json.JSONDecodeError(
                f"Failed to decode JSON response from {endpoint}: {str(e)}",
                e.doc,
                e.pos
            )
        except HTTPError as e:
            logging.error(f"HTTP error creating organizational unit '{name}': {str(e)}")
            if e.response.status_code == 400:
                # Try to create with a simplified/sanitized title if there's a Bad Request
                simplified_name = ''.join(c for c in name if c.isalnum() or c.isspace()).strip()
                if simplified_name and simplified_name != name:
                    logging.info(f"Attempting to create with simplified name: '{simplified_name}'")
                    org_unit_data["label"] = simplified_name
                    try:
                        response = requests_post(endpoint, headers=headers, json=org_unit_data, rate_limit_delay=self.request_delay)
                        
                        # Cache the newly created unit UUID if caching enabled
                        if self.uuid_cache:
                            try:
                                unit_uuid = response.json()['id']
                                if unit_uuid:
                                    # Construct proper path for the unit
                                    if parent_name is None:
                                        unit_path = url_join('rest', self.database_name, 'schemes', self.dnk_scheme_name, 'collections', simplified_name)
                                    else:
                                        unit_path = response.json()['_links']['self']['href']
                                        
                                    self.uuid_cache.add_or_update_asset('OrganizationalUnit', simplified_name, unit_uuid, unit_path)
                            except Exception as e:
                                logging.warning(f"Failed to cache organizational unit UUID: {str(e)}")
                        
                        if parent_name:
                            logging.info(f"Organizational unit created with simplified name '{simplified_name}' under '{parent_name}'")
                        else:
                            logging.info(f"Top-level organizational unit created with simplified name '{simplified_name}'")
                        return
                    except Exception as e2:
                        logging.error(f"Failed with simplified name too: {str(e2)}")
            raise

    # Maintaining these methods for backward compatibility, but they now delegate to dnk_create_or_update_organizational_unit
    def dnk_create_new_department(self, name: str, custom_properties: dict = None) -> None:
        """
        Create a new department in the 'Datennutzungskatalog' of Dataspot. If the department already exists, do nothing.
        
        This is a backward compatibility method that delegates to dnk_create_or_update_organizational_unit.

        Args:
            name (str): The name of the department.
            custom_properties (dict): Custom properties to be added to the department.

        Returns:
            None: The method doesn't return anything.
        """
        logging.warning("dnk_create_new_department is deprecated, use dnk_create_or_update_organizational_unit instead")
        self.dnk_create_or_update_organizational_unit(name, parent_name=None, custom_properties=custom_properties)

    def dnk_create_new_dienststelle(self, name: str, belongs_to_department: str, custom_properties: dict = None) -> None:
        """
        Create a new "dienststelle" in the 'Datennutzungskatalog' in Dataspot under a specific department. 
        If the "dienststelle" already exists, do nothing.
        
        This is a backward compatibility method that delegates to dnk_create_or_update_organizational_unit.

        Args:
            name (str): The name of the dienststelle.
            belongs_to_department (str): The title of the parent department.
            custom_properties (dict): Custom properties to be added to the dienststelle.
        """
        logging.warning("dnk_create_new_dienststelle is deprecated, use dnk_create_or_update_organizational_unit instead")
        self.dnk_create_or_update_organizational_unit(name, parent_name=belongs_to_department, custom_properties=custom_properties)

    def dnk_create_new_sammlung(self, title: str, belongs_to_dienststelle: str, custom_properties: dict = None) -> None:
        """
        Create a new sammlung in the 'Datennutzungskatalog' in Dataspot under a specific dienststelle. 
        If the sammlung already exists, do nothing.
        
        This is a backward compatibility method that delegates to dnk_create_or_update_organizational_unit.

        Args:
            title (str): The title of the sammlung.
            belongs_to_dienststelle (str): The name of the parent dienststelle.
            custom_properties (dict): Custom properties to be added to the sammlung.
        """
        logging.warning("dnk_create_new_sammlung is deprecated, use dnk_create_or_update_organizational_unit instead")
        self.dnk_create_or_update_organizational_unit(title, parent_name=belongs_to_dienststelle, custom_properties=custom_properties)

    def dnk_create_or_update_dataset(self, dataset: Dataset, update_strategy: str = 'create_or_update', force_replace: bool = False) -> dict:
        """
        Create a new dataset or update an existing dataset in the 'Datennutzungskatalog/ODS-Imports' in Dataspot.
        All datasets are placed directly in the ODS-Imports collection, regardless of their internal path structure.
        
        The method behavior is controlled by the update_strategy parameter:
        - 'create_only': Only creates a new dataset using POST. Fails if the dataset already exists.
        - 'update_only': Only updates an existing dataset. Fails if the dataset doesn't exist.
        - 'create_or_update' (default): Creates a new dataset if it doesn't exist, updates it if it does.
        
        The force_replace parameter controls the update behavior:
        - False (default): Uses PATCH to update only the specified properties, preserving other properties.
        - True: Uses PUT to completely replace the dataset with the new values.

        Args:
            dataset (Dataset): The dataset instance to be uploaded.
            update_strategy (str): Strategy for handling dataset existence ('create_only', 'update_only', 'create_or_update').
            force_replace (bool): Whether to completely replace an existing dataset (True) or just update properties (False).
            
        Returns:
            dict: The JSON response from the API containing the dataset data
            
        Raises:
            ValueError: If the update_strategy is invalid
            HTTPError: If API requests fail
            json.JSONDecodeError: If response parsing fails
        """
        if update_strategy not in ['create_only', 'update_only', 'create_or_update']:
            raise ValueError(f"Invalid update_strategy: {update_strategy}. Must be one of: 'create_only', 'update_only', 'create_or_update'")
        
        headers = self.auth.get_headers()
        dataset_exists = False
        dataset_url = None
        escaped_name = escape_special_chars(dataset.name)
        
        # 1. Try to find dataset directly using href from cache first
        if self.uuid_cache:
            dataset_href = self.uuid_cache.get_href('Dataset', dataset.name)
            if dataset_href:
                dataset_url = url_join(self.base_url, dataset_href)
                try:
                    response = requests_get(dataset_url, headers=headers, rate_limit_delay=self.request_delay)
                    dataset_exists = True
                    # Update UUID in cache if needed
                    dataset_uuid = response.json()['id']
                    if dataset_uuid and self.uuid_cache:
                        self.uuid_cache.add_or_update_asset('Dataset', dataset.name, dataset_uuid, dataset_href)
                    logging.info(f"Dataset '{dataset.name}' found using cached href.")
                except HTTPError as e:
                    # Href is no longer valid, will need to search by name
                    logging.debug(f"Dataset href from cache is invalid, will search by direct path: {str(e)}")
                    dataset_url = None
        
        # TODO (renato): Think about these comments. What exactly should happen? Do I really want Step 2 below???
        # If a dataset exists and has a valid href, that href should remain valid as long as the dataset exists. If a request to that href fails, it likely means one of:
        # The dataset was deleted
        # The user's permissions changed
        # There's a server-side issue
        # The cache is corrupted with a malformed href

        # The more common and valid scenario is when a dataset exists but isn't in your cache:
        # Created by another user
        # Created through the web interface
        # Created by a different client instance
        # Created before you started using caching

        # 2. If not found in cache, try to find by direct path
        if not dataset_exists:
            # Use pattern (1) from user instructions: /rest/{database}/schemes/{scheme}/datasets/{name}
            dataset_path = url_join('rest', self.database_name, 'schemes', self.dnk_scheme_name, 'datasets', escaped_name)
            dataset_url = url_join(self.base_url, dataset_path)
            
            try:
                response = requests_get(dataset_url, headers=headers, rate_limit_delay=self.request_delay)
                dataset_data = response.json()
                dataset_exists = True
                
                # Cache the dataset information
                if self.uuid_cache:
                    dataset_uuid = dataset_data['id']
                    dataset_href = dataset_data['_links']['self']['href']
                    if dataset_uuid and dataset_href:
                        self.uuid_cache.add_or_update_asset('Dataset', dataset.name, dataset_uuid, dataset_href)
                
                logging.info(f"Dataset '{dataset.name}' found by direct path.")
            except HTTPError as e:
                if e.response.status_code != 404:
                    logging.error(f"Error searching for dataset by direct path: {str(e)}")
                    raise
                logging.info(f"Dataset '{dataset.name}' does not exist in the DNK scheme.")
        
        # 3. Handle the dataset based on existence and update strategy
        if dataset_exists:
            if update_strategy == 'create_only':
                logging.info(f"Dataset '{dataset.name}' already exists and update_strategy is 'create_only'. Skipping.")
                # Return the latest dataset data
                try:
                    response = requests_get(dataset_url, headers=headers, rate_limit_delay=self.request_delay)
                    return response.json()
                except HTTPError as e:
                    logging.error(f"Error fetching existing dataset '{dataset.name}': {str(e)}")
                    raise
            
            # Update existing dataset using PUT (replace) or PATCH (update) based on force_replace
            dataset_json = dataset.to_json()
            try:
                if force_replace:
                    # Use PUT to completely replace the dataset
                    logging.info(f"Replacing dataset '{dataset.name}' using PUT")
                    response = requests_put(dataset_url, headers=headers, json=dataset_json, rate_limit_delay=self.request_delay)
                else:
                    # Use PATCH to update only the specified properties
                    logging.info(f"Updating dataset '{dataset.name}' using PATCH")
                    response = requests_patch(dataset_url, headers=headers, json=dataset_json, rate_limit_delay=self.request_delay)
                
                # Update cache with the latest href and UUID from the response
                if self.uuid_cache and response.status_code in [200, 201]:
                    response_data = response.json()
                    updated_uuid = response_data['id']
                    updated_href = response_data['_links']['self']['href']
                    if updated_uuid and updated_href:
                        self.uuid_cache.add_or_update_asset('Dataset', dataset.name, updated_uuid, updated_href)
                
                logging.info(f"Dataset '{dataset.name}' updated successfully")
                return response.json()
            except HTTPError as e:
                logging.error(f"Error updating dataset '{dataset.name}': {str(e)}")
                raise
        
        # 4. Handle update_only case
        if update_strategy == 'update_only':
            raise ValueError(f"Dataset '{dataset.name}' doesn't exist and update_strategy is 'update_only'")
        
        # 5. Create dataset if it doesn't exist
        # First, ensure ODS-Imports collection exists
        collection_data = self.ensure_ods_imports_collection()
        ods_imports_uuid = collection_data['id']
        
        # Prepare dataset JSON for creation
        dataset_json = dataset.to_json()
        # Ensure the dataset is created in the ODS-Imports collection
        dataset_json["inCollection"] = ods_imports_uuid
        
        # Create endpoint for dataset creation
        assets_endpoint = url_join('rest', self.database_name, 'assets')
        assets_url = url_join(self.base_url, assets_endpoint)
        
        try:
            # Create new dataset using POST
            logging.info(f"Creating dataset '{dataset.name}' using POST")
            response = requests_post(assets_url, headers=headers, json=dataset_json, rate_limit_delay=self.request_delay)
            
            # Only update cache if creation was successful
            if response.status_code in [200, 201]:
                response_data = response.json()
                new_dataset_uuid = response_data['id']
                if new_dataset_uuid and self.uuid_cache:
                    href = response_data['_links']['self']['href']
                    self.uuid_cache.add_or_update_asset('Dataset', dataset.name, new_dataset_uuid, href)
            
            logging.info(f"Dataset '{dataset.name}' created successfully")
            return response.json()
        except HTTPError as e:
            logging.error(f"Error creating dataset '{dataset.name}': {str(e)}")
            # TODO (renato): When we encounter other causes for error code 400, append them here!
            # If we get a 400 error, it might be due to special characters in the name
            if e.response.status_code == 400:
                logging.warning(f"Bad request when creating dataset. This might be due to special characters in the name.")
            raise

    def ensure_ods_imports_collection(self) -> dict:
        """
        Ensures that the ODS-Imports collection exists within the Datennutzungskatalog scheme.
        Creates both the scheme and collection if they don't exist.
        
        Returns:
            dict: The JSON response containing the UUID of the ODS-Imports collection
        """
        headers = self.auth.get_headers()
        
        # First check if we have the UUID in cache
        if self.uuid_cache:
            collection_uuid = self.uuid_cache.get_uuid('Collection', self.ods_imports_collection_name)
            if collection_uuid:
                # Verify the cached UUID is still valid
                collection_endpoint = url_join('rest', self.database_name, 'collections', collection_uuid)
                try:
                    response = requests_get(url_join(self.base_url, collection_endpoint), headers=headers, rate_limit_delay=self.request_delay)
                    if response.status_code == 200:
                        return response.json()
                except HTTPError:
                    # Cache is invalid, will proceed with normal lookup
                    pass
                
        # Check if DNK scheme exists and create if needed
        dnk_scheme_path = url_join('rest', self.database_name, 'schemes', self.dnk_scheme_name)
        dnk_scheme_endpoint = url_join(self.base_url, dnk_scheme_path)
        
        try:
            response = requests_get(dnk_scheme_endpoint, headers=headers, rate_limit_delay=self.request_delay)
            dnk_scheme_uuid = response.json()['id']
            logging.debug(f"DNK scheme exists with UUID: {dnk_scheme_uuid}")
        except HTTPError as e:
            if e.response.status_code == 404:
                # DNK scheme doesn't exist, create it
                logging.info("DNK scheme doesn't exist, creating it")
                schemes_endpoint = url_join(self.base_url, 'rest', self.database_name, 'schemes')
                scheme_data = {
                    "_type": "Scheme",
                    "label": self.dnk_scheme_name
                }
                response = requests_post(schemes_endpoint, headers=headers, json=scheme_data, rate_limit_delay=self.request_delay)
                dnk_scheme_uuid = response.json()['id']
                logging.info(f"Created DNK scheme with UUID: {dnk_scheme_uuid}")
            else:
                raise
        
        # Check if ODS-Imports collection exists and create if needed
        ods_imports_path = url_join('rest', self.database_name, 'schemes', self.dnk_scheme_name, 'collections', self.ods_imports_collection_name)
        ods_imports_endpoint = url_join(self.base_url, ods_imports_path)
        
        try:
            response = requests_get(ods_imports_endpoint, headers=headers, rate_limit_delay=self.request_delay)
            ods_imports_uuid = response.json()['id']
            
            # Only cache the UUID if the response was successful
            if self.uuid_cache and response.status_code == 200:
                ods_imports_path = response.json()['_links']['self']['href']
                if ods_imports_path:
                    self.uuid_cache.add_or_update_asset('Collection', self.ods_imports_collection_name, ods_imports_uuid, ods_imports_path)
                
            logging.debug(f"ODS-Imports collection exists with UUID: {ods_imports_uuid}")
            return response.json()
        except HTTPError as e:
            if e.response.status_code == 404:
                # ODS-Imports doesn't exist, create it
                logging.info("ODS-Imports collection doesn't exist, creating it")
                collections_endpoint = url_join(self.base_url, 'rest', self.database_name, 'schemes', dnk_scheme_uuid, 'collections')
                collection_data = {
                    "_type": "Collection",
                    "label": self.ods_imports_collection_name
                }
                try:
                    response = requests_post(collections_endpoint, headers=headers, json=collection_data, rate_limit_delay=self.request_delay)
                    ods_imports_uuid = response.json()['id']
                    
                    # Only cache if creation was successful
                    if self.uuid_cache and response.status_code in [200, 201]:
                        ods_imports_href = response.json()['_links']['self']['href']
                        if ods_imports_href:
                            self.uuid_cache.add_or_update_asset('Collection', self.ods_imports_collection_name, ods_imports_uuid, ods_imports_href)
                        
                    logging.info(f"Created ODS-Imports collection with UUID: {ods_imports_uuid}")
                    return response.json()
                except HTTPError as create_error:
                    logging.error(f"Failed to create ODS-Imports collection: {str(create_error)}")
                    raise
            else:
                raise

    def create_hierarchy_for_dataset(self, dataset: Dataset) -> None:
        """
        [DEPRECATED]
        Creates the complete hierarchy (organizational units) for a dataset if it doesn't exist.
        
        .. deprecated:: Will be removed in a future version
        
        Args:
            dataset (Dataset): The dataset containing the path information
            
        Raises:
            ValueError: If the path is invalid
        """
        logging.warning("The method create_hierarchy_for_dataset is deprecated.")
        departement, dienststelle, sammlung, subsammlung = dataset.get_departement_dienststelle_sammlung_subsammlung()
        
        if not departement:
            raise ValueError("Top-level organizational unit is required")
            
        # Create top-level organizational unit if it doesn't exist
        self.dnk_create_or_update_organizational_unit(departement)
        
        # Create second-level organizational unit if specified and doesn't exist
        if dienststelle:
            self.dnk_create_or_update_organizational_unit(dienststelle, parent_name=departement)
            
            # Create third-level organizational unit if specified and doesn't exist
            if sammlung:
                self.dnk_create_or_update_organizational_unit(sammlung, parent_name=dienststelle)
                
                # Create fourth-level organizational unit if specified and doesn't exist
                if subsammlung:
                    self.dnk_create_or_update_organizational_unit(subsammlung, parent_name=sammlung)

    def link_dnk_bestandteile_to_tdm(self, title: str) -> None:
        """
        Links DNK dataset to TDM attributes by adding composition objects.
        For each attribute in the TDM, this creates a "Bestandteil" (composition) 
        in the DNK element.
        
        Args:
            title (str): The title/name of the dataset
            
        Raises:
            HTTPError: If API requests fail
            ValueError: If dataset or attributes are not found
            json.JSONDecodeError: If response parsing fails
        """
        logging.info(f"Linking DNK to TDM for dataset: {title}")
        headers = self.auth.get_headers()
        
        # 1. Find the DNK dataset (asset) by title
        dataset_path = self.find_dataset_path(title)
        dataset_endpoint = url_join(self.base_url, dataset_path)

        try:
            response = requests_get(dataset_endpoint, headers=headers)
            dataset_data = response.json()

            dataset_href = dataset_data['_links']['self']['href']
            dataset_id = dataset_data['id']

            if not dataset_href:
                raise ValueError(f"Dataset with title '{title}' not found in DNK")
            
            logging.debug(f"Found DNK dataset: {dataset_id}")
        except HTTPError as e:
            logging.error(f"Failed to fetch DNK scheme: {str(e)}")
            raise
        
        # 2. Find TDM attributes for this dataset
        tdm_attributes_path = url_join(self.find_tdm_dataobject_path(title), 'attributes')
        tdm_attributes_endpoint = url_join(self.base_url, tdm_attributes_path)
        
        try:
            # Get TDM attributes of asset with matching name
            response = requests_get(tdm_attributes_endpoint, headers=headers)
            tdm_attributes_data = response.json()

            tdm_attributes = tdm_attributes_data.get('_embedded', {}).get('attributes', {})
            
            if not tdm_attributes:
                logging.warning(f"No attributes found for TDM dataobject '{title}'")
                return
            
            logging.debug(f"Found {len(tdm_attributes)} TDM attributes")
        except HTTPError as e:
            logging.error(f"Failed to fetch TDM dataobject attributes: {str(e)}")
            raise
        
        # 3. Create compositions in DNK for each TDM attribute
        compositions_endpoint = url_join(self.base_url, dataset_href, 'compositions')
        
        try:
            # Add a composition for each attribute
            for attribute in tdm_attributes:
                attribute_label = attribute.get('title')
                attribute_id = attribute['id']
                
                if not attribute_label:
                    logging.warning(f"Skipping attribute with missing label: {attribute}")
                    continue
                
                # Create composition object
                composition_data = {
                    "_type": "Composition",
                    "label": attribute_label,
                    "composedOf": attribute_id
                }
                
                # Check if composition already exists
                try:
                    existing_response = requests_get(
                        url_join(compositions_endpoint, attribute_label), 
                        headers=headers
                    )
                    logging.info(f"Composition for '{attribute_label}' already exists. Skipping...")
                    continue
                except HTTPError as e:
                    if e.response.status_code != 404:
                        raise
                
                # Add the composition
                requests_post(compositions_endpoint, headers=headers, json=composition_data)
                logging.info(f"Created composition for attribute '{attribute_label}'")
            
            logging.info(f"Successfully linked DNK dataset '{title}' to TDM attributes")
        except HTTPError as e:
            logging.error(f"Failed to create composition: {str(e)}")
            raise

    def find_dataset_path(self, title):
        """
        Find the path to a dataset in the DNK scheme by its title.
        
        Args:
            title (str): The title or name of the dataset
            
        Returns:
            str: The full path to the dataset
            
        Raises:
            ValueError: If the dataset is not found
        """
        logging.warning("The method find_dataset_path is deprecated.")
        # Check cache first if caching is enabled
        if self.uuid_cache:
            cached_href = self.uuid_cache.get_href('Dataset', title)
            if cached_href:
                return cached_href

        if '/' not in title:
            # If no slash, construct the path using the standard format
            dataset_path = url_join('rest', self.database_name, 'schemes', self.dnk_scheme_name, 'collections', self.ods_imports_collection_name, 'datasets', title)
            
            # Verify the path exists and get the UUID
            try:
                endpoint = url_join(self.base_url, dataset_path)
                response = requests_get(endpoint, headers=self.auth.get_headers())
                dataset_uuid = response.json()['id']
                
                # Cache the result if caching is enabled
                if self.uuid_cache and dataset_uuid:
                    self.uuid_cache.add_or_update_asset('Dataset', title, dataset_uuid, dataset_path)
                
                return dataset_path
            except HTTPError:
                pass  # Continue with the search if not found with direct path
                
        # If there is a slash, then we need to find the dataset by title
        datasets_path = url_join('rest', self.database_name, 'schemes', self.dnk_scheme_name, 'collections', self.ods_imports_collection_name, 'datasets')
        datasets_endpoint = url_join(self.base_url, datasets_path)
        response = requests_get(datasets_endpoint, headers=self.auth.get_headers())
        datasets_data = response.json()
        datasets = datasets_data.get('_embedded', {}).get('datasets', [])
        
        for dataset in datasets:
            if dataset.get('label') == title:
                path = dataset['_links']['self']['href']
                uuid = dataset['id']
                
                # Cache the result if caching is enabled and UUID exists
                if self.uuid_cache and uuid:
                    self.uuid_cache.add_or_update_asset('Dataset', title, uuid, path)
                
                return path
                
        raise ValueError(f"Dataset with title '{title}' not found in DNK")

    def find_tdm_dataobject_path(self, title):
        """
        Determine the path to TDM attributes for a dataset based on its title.
        
        Args:
            title (str): The title or name of the dataset
            
        Returns:
            str: The full path to the TDM attributes
            
        Raises:
            ValueError: If the TDM object is not found
        """
        logging.warning("The method find_tdm_dataobject_path is deprecated.")
        # Check cache first if caching is enabled
        if self.uuid_cache:
            cached_href = self.uuid_cache.get_href('TDMDataobject', title)
            if cached_href:
                return cached_href
                
        if '/' not in title:
            # If no slash, construct the path using the standard format
            tdm_path = url_join('rest', self.database_name, 'schemes', self.tdm_scheme_name, 'collections', self.ods_imports_collection_name, 'classifiers', title)
            
            # Verify the path exists and get the UUID
            try:
                endpoint = url_join(self.base_url, tdm_path)
                response = requests_get(endpoint, headers=self.auth.get_headers())
                tdm_uuid = response.json()['id']
                
                # Cache the result if caching is enabled
                if self.uuid_cache and tdm_uuid:
                    self.uuid_cache.add_or_update_asset('TDMDataobject', title, tdm_uuid, tdm_path)
                
                return tdm_path
            except HTTPError:
                pass  # Continue with the search if not found with direct path
                
        # If there is a slash, we need to find the TDM object by title
        assets_path = url_join('rest', self.database_name, 'schemes', self.tdm_scheme_name, 'collections', self.ods_imports_collection_name, 'assets')
        assets_endpoint = url_join(self.base_url, assets_path)
        response = requests_get(assets_endpoint, headers=self.auth.get_headers())
        assets_data = response.json()
        assets = assets_data.get('_embedded', {}).get('assets', [])
        
        for asset in assets:
            if asset.get('label') == title:
                path = asset['_links']['self']['href']
                uuid = asset['id']
                
                # Cache the result if caching is enabled
                if self.uuid_cache and uuid:
                    self.uuid_cache.add_or_update_asset('TDMDataobject', title, uuid, path)
                
                return path
                
        raise ValueError(f"TDM dataobject with title '{title}' not found")

    def delete_dataset(self, title: str, fail_if_not_exists: bool = False, delete_tdm_asset: bool = True) -> bool:
        """
        Delete a dataset from the DNK scheme.
        
        Args:
            title (str): The title/name of the dataset to be deleted
            fail_if_not_exists (bool): Whether to raise an error if the dataset doesn't exist
            delete_tdm_asset (bool): Whether to also delete the corresponding TDM dataobject (if exists)
            
        Returns:
            bool: True if the dataset was deleted, False if it didn't exist and fail_if_not_exists is False
            
        Raises:
            ValueError: If the dataset doesn't exist and fail_if_not_exists is True
            HTTPError: If API requests fail
            json.JSONDecodeError: If response parsing fails
        """
        headers = self.auth.get_headers()
        
        logging.info(f"Deleting dataset: {title}")
        
        # Find the dataset path
        try:
            dataset_path = self.find_dataset_path(title)
            dataset_endpoint = url_join(self.base_url, dataset_path)
            
            # Verify the dataset exists by fetching it
            response = requests_get(dataset_endpoint, headers=headers)
            dataset_data = response.json()
            dataset_uuid = dataset_data['id']
            
            if not dataset_uuid:
                raise ValueError(f"Dataset '{title}' found but has no UUID")
                
            logging.debug(f"Found dataset to delete: {dataset_uuid}")
                
        except (ValueError, HTTPError) as e:
            if isinstance(e, HTTPError) and e.response.status_code != 404:
                # Re-raise if it's an error other than "not found"
                raise
                
            # Handle the case where the dataset doesn't exist
            if fail_if_not_exists:
                raise ValueError(f"Dataset '{title}' not found and fail_if_not_exists is True")
            else:
                logging.info(f"Dataset '{title}' not found. Nothing to delete.")
                return False
        
        # Delete the dataset
        try:
            requests_delete(dataset_endpoint, headers=headers, rate_limit_delay=self.request_delay)
            
            # Remove from UUID cache if caching is enabled
            if self.uuid_cache:
                try:
                    # Remove the dataset from the cache
                    self.uuid_cache.remove_asset('Dataset', title)
                except Exception as e:
                    logging.warning(f"Failed to remove dataset from UUID cache: {str(e)}")
            
            logging.info(f"Successfully deleted DNK dataset '{title}'")
        except HTTPError as e:
            logging.error(f"Failed to delete dataset '{title}': {str(e)}")
            raise
            
        # If requested, also delete the TDM dataobject
        if delete_tdm_asset:
            try:
                # Find the TDM dataobject
                dataobject_path = self.find_tdm_dataobject_path(title)
                dataobject_endpoint = url_join(self.base_url, dataobject_path)
                
                # Check if TDM dataobject exists
                try:
                    asset_response = requests_get(dataobject_endpoint, headers=headers)
                    # Delete the TDM dataobject
                    requests_delete(dataobject_endpoint, headers=headers)
                    
                    # Remove from UUID cache if caching is enabled
                    if self.uuid_cache:
                        try:
                            # Remove the TDM dataobject from the cache
                            self.uuid_cache.remove_asset('TDMDataobject', title)
                        except Exception as e:
                            logging.warning(f"Failed to remove TDM dataobject from UUID cache: {str(e)}")
                    
                    logging.info(f"Successfully deleted TDM dataobject '{title}'")
                except HTTPError as e:
                    if e.response.status_code == 404:
                        logging.info(f"No TDM dataobject found for '{title}'. Nothing to delete.")
                    else:
                        logging.error(f"Failed to delete TDM dataobject for '{title}': {str(e)}")
                        raise
            except Exception as e:
                # Log but don't fail if TDM deletion fails
                logging.warning(f"Error deleting TDM dataobject for '{title}': {str(e)}")
                
        return True

    def find_datatype_path(self, type_name):
        """
        Find the path to a datatype in the Datentypmodell scheme by its name.
        
        Args:
            type_name (str): The name of the datatype
            
        Returns:
            str: The full path to the datatype
            
        Raises:
            ValueError: If the datatype is not found
        """
        logging.warning("The method find_datatype_path is deprecated.")
        # Check cache first if caching is enabled
        if self.uuid_cache:
            cached_href = self.uuid_cache.get_href('Datatype', type_name)
            if cached_href:
                return cached_href
                
        if '/' not in type_name:
            # If no slash, construct the path using the standard format
            datatype_path = url_join('rest', self.database_name, 'schemes', self.datatype_scheme_name, 'datatypes', type_name)
            
            # Verify the path exists and get the UUID
            try:
                endpoint = url_join(self.base_url, datatype_path)
                response = requests_get(endpoint, headers=self.auth.get_headers())
                datatype_uuid = response.json()['id']
                
                # Cache the result if caching is enabled
                if self.uuid_cache and datatype_uuid:
                    self.uuid_cache.add_or_update_asset('Datatype', type_name, datatype_uuid, datatype_path)
                
                return datatype_path
            except HTTPError:
                pass  # Continue with the search if not found with direct path
                
        # If there is a slash, we need to find the datatype by name
        datatypes_path = url_join('rest', self.database_name, 'schemes', self.datatype_scheme_name, 'datatypes')
        datatypes_endpoint = url_join(self.base_url, datatypes_path)
        response = requests_get(datatypes_endpoint, headers=self.auth.get_headers())
        datatypes_data = response.json()
        datatypes = datatypes_data.get('_embedded', {}).get('datatypes', [])
        
        for datatype in datatypes:
            if datatype.get('label') == type_name:
                path = datatype['_links']['self']['href']
                uuid = datatype['id']
                
                # Cache the result if caching is enabled
                if self.uuid_cache and uuid:
                    self.uuid_cache.add_or_update_asset('Datatype', type_name, uuid, path)
                
                return path
                
        raise ValueError(f"Datatype with name '{type_name}' not found")

    def build_organization_hierarchy_from_ods(self, org_data: dict, cooldown_delay: float = 1.0):
        """
        Build organization hierarchy in Dataspot based on organization data from ODS API.
        
        This method processes organizations by their path depth (from title_full) to ensure
        that parent organizations are always created before their children. Within each depth
        level, it follows a breadth-first approach to process organizations.
        
        Args:
            org_data (dict): Dictionary containing organization data from ODS API
            cooldown_delay (float): Delay in seconds between API calls to prevent overloading the server (default: 1.0)
            
        Returns:
            None
        
        Raises:
            ValueError: If organization data is missing or invalid
        """
        logging.warning("The method build_organization_hierarchy_from_ods is deprecated.")
        if not org_data or 'results' not in org_data:
            raise ValueError("Invalid organization data format")
            
        # Build a lookup dictionary for quick access to organization by ID
        org_lookup = {org['id']: org for org in org_data['results']}
        
        # Keep track of processed organizations to avoid duplicates
        processed_orgs = set()
        failed_orgs = set()
        
        # Group organizations by their path depth
        depth_groups = {}
        for org_id, org in org_lookup.items():
            title_full = org.get('title_full', '')
            if not title_full:
                continue
                
            # Determine path depth (number of segments in title_full)
            path_depth = len(title_full.split('/'))
            if path_depth not in depth_groups:
                depth_groups[path_depth] = []
                
            depth_groups[path_depth].append(org_id)
        
        # Calculate total number of organizations to process
        total_orgs = sum(len(depth_groups[depth]) for depth in depth_groups)
        current_org = 0
        
        # Process organizations level by level (starting from the shallowest)
        for depth in sorted(depth_groups.keys()):
            logging.info(f"Processing organizations at depth level {depth}")
            
            # Use a queue for processing organizations at this depth level
            from collections import deque
            queue = deque(depth_groups[depth])
            
            while queue:
                org_id = queue.popleft()
                
                # Skip if already processed or failed before
                if org_id in processed_orgs or org_id in failed_orgs:
                    continue
                
                # Skip if not found in lookup
                if org_id not in org_lookup:
                    logging.warning(f"Organization with ID {org_id} not found in data, skipping")
                    failed_orgs.add(org_id)
                    continue
                
                org = org_lookup[org_id]

                # Extract organization details
                title = org.get('title')
                title_full = org.get('title_full', '')
                children_ids = org.get('children_id', []) or []
                url_website = generate_potential_staatskalender_url(title_full)

                # Increment the counter
                current_org += 1

                # Check if url is valid
                try:
                    response = requests_get(url_website)
                    if not response.status_code == 200:
                        logging.warning(f"Invalid URL for organization '{title}': {url_website}.")
                        url_website = org.get('url_website', '')
                        logging.info(f"Invalid URL for organization '{title}', falling back to {url_website}")
                except Exception as e:
                    logging.warning(f"Invalid URL for organization '{title}': {url_website}")

                    # Note: We don't check whether the fallback url is valid, because we assume it is.
                    url_website = org.get('url_website', '')
                    logging.info(f"Invalid URL for organization '{title}', falling back to {url_website}")

                # Create custom properties for the organization
                custom_properties = {
                    "ID": org_id,
                    "Link_zum_Staatskalender": url_website
                }
                
                # Skip if organization is missing critical data
                if not title or not title_full:
                    logging.warning(f"Organization with ID {org_id} has missing data, skipping")
                    failed_orgs.add(org_id)
                    continue
                
                # Extract the path components from title_full
                path_components = title_full.split('/')
                
                logging.info(f"[{current_org}/{total_orgs}] Processing organization: {title} (ID: {org_id}, Path: {title_full})")
                
                try:
                    # Add cooldown delay before making API calls to create/update the organization
                    if cooldown_delay > 0:
                        sleep(cooldown_delay)
                    
                    # Create the organization based on its level in the hierarchy
                    try:
                        if len(path_components) == 1:
                            # Top level organization unit (no parent)
                            self.dnk_create_or_update_organizational_unit(title, parent_name=None, custom_properties=custom_properties)
                        elif len(path_components) == 2:
                            # Second level organization unit
                            parent_title = path_components[0]
                            self.dnk_create_or_update_organizational_unit(title, parent_name=parent_title, custom_properties=custom_properties)
                        elif len(path_components) == 3:
                            # Third level organization unit
                            parent_title = path_components[1]
                            logging.debug(f"Creating organizational unit '{title}' under parent '{parent_title}'")
                            self.dnk_create_or_update_organizational_unit(title, parent_name=parent_title, custom_properties=custom_properties)
                        elif len(path_components) >= 4:
                            # Fourth level organization unit and beyond
                            parent_title = path_components[2]
                            self.dnk_create_or_update_organizational_unit(title, parent_name=parent_title, custom_properties=custom_properties)
                    except HTTPError as e:
                        if e.response.status_code == 400:
                            logging.error(f"Bad request error creating organization '{title}': {str(e)}")
                            
                            # Try a simplified approach - create a top-level organizational unit regardless of depth
                            try:
                                logging.info(f"[{current_org}/{total_orgs}] Attempting to create '{title}' as a top-level organizational unit as fallback")
                                self.dnk_create_or_update_organizational_unit(title, parent_name=None, custom_properties=custom_properties)
                                logging.info(f"[{current_org}/{total_orgs}] Successfully created '{title}' as a top-level organizational unit")
                            except Exception as fallback_e:
                                logging.error(f"Fallback creation also failed: {str(fallback_e)}")
                                raise
                        else:
                            raise
                    
                    # Mark as processed
                    processed_orgs.add(org_id)
                    
                except Exception as e:
                    logging.error(f"Error processing organization {org_id}: {str(e)}")
                    failed_orgs.add(org_id)
        
        # Log summary statistics
        logging.info(f"Processed {len(processed_orgs)} organizations successfully")
        if failed_orgs:
            logging.warning(f"Failed to process {len(failed_orgs)} organizations")
