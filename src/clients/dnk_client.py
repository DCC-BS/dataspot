import logging
import os
from typing import Dict, List, Any, Optional, Tuple

from requests import HTTPError

from src.clients.base_client import BaseDataspotClient
from src.clients.helpers import url_join, escape_special_chars, generate_potential_staatskalender_url
from src.dataspot_auth import DataspotAuth
from src.dataspot_dataset import Dataset
from src.common import requests_get
from src.ods_dataspot_mapping import ODSDataspotMapping


# TODO (large language model): If possible, remove any headers and authentication from this class, and handle all of that (and, in particular, headers) in the BaseDataspotClient class.
class DNKClient(BaseDataspotClient):
    """Client for interacting with the DNK (Datennutzungskatalog)."""
    
    def __init__(self, auth: Optional[DataspotAuth] = None, request_delay: float = 1.0, mapping_file: str = "ods_dataspot_mapping.csv"):
        """
        Initialize the DNK client.
        
        Args:
            auth (DataspotAuth, optional): Authentication provider instance. If None, creates a new one.
            request_delay (float, optional): Delay between API requests in seconds. Default is 1.0 second.
            mapping_file (str, optional): Path to the CSV file for ODS-Dataspot mapping. Default is "ods_dataspot_mapping.csv".
        """
        super().__init__(request_delay=request_delay)
        # TODO (large language model): For any of the os.getenvs: Do not default to any value, but throw an error if not found!
        self.dnk_scheme_name = os.getenv("DATASPOT_DNK_SCHEME_NAME", 'Datennutzungskatalog')
        self.ods_imports_collection_name = os.getenv("DATASPOT_ODS_IMPORTS_COLLECTION_NAME", 'ODS-Imports')
        self.mapping = ODSDataspotMapping(mapping_file)

    # TODO: Do NOT implement me
    def teardown_dnk(self, delete_empty_collections: bool = False) -> None:
        """
        Delete all OGD datasets and optionally empty collections from the DNK.
        
        Args:
            delete_empty_collections (bool): Whether to delete empty collections (default: False)
            
        Raises:
            HTTPError: If the API request fails
        """
        pass

    # TODO (large language model): Add logging.
    # TODO (large language model): Check with docs. Try to access the dataset from the mapping. If it already exists, directly access it through the href. NEVER use the name to access a dataset.
    def create_or_update_dataset(self, dataset: Dataset, update_strategy: str = 'create_or_update',
                                     force_replace: bool = False) -> dict:
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
        # Validate update strategy
        valid_strategies = ['create_only', 'update_only', 'create_or_update']
        if update_strategy not in valid_strategies:
            raise ValueError(f"Invalid update_strategy: {update_strategy}. Must be one of {valid_strategies}")
        
        # Get ODS ID from dataset
        ods_id = dataset.properties.get('ID')
        if not ods_id:
            raise ValueError("Dataset must have an 'ID' property to use as ODS ID")
        
        # Escape the dataset title for use in business key
        escaped_title = escape_special_chars(dataset.title)
        
        # TODO (Renato): Think about whether it would make sense to also have a lookup table for the schemes.
        # Construct base endpoint for the collection where datasets are stored
        collection_endpoint = url_join(
            self.base_url,
            "rest",
            self.database_name,
            "schemes",
            self.dnk_scheme_name,
            "collections",
            self.ods_imports_collection_name
        )
        
        # Construct the endpoint for this specific dataset
        dataset_endpoint = url_join(collection_endpoint, escaped_title)
        
        # Check if dataset exists in Dataspot
        dataset_exists = False
        href = None
        
        # Check mapping for existing entry
        entry = self.mapping.get_entry(ods_id)
        if entry:
            dataset_exists = True
            _, href = entry
            # Verify that the dataset still exists at this href
            try:
                self.resource_exists(href)
                
            # TODO (renato): Check if this is the correct exception to catch.
            except HTTPError:
                # If we get an error, the dataset might have been moved or deleted
                dataset_exists = False
                self.mapping.remove_entry(ods_id)
        
        # If not in mapping or mapping was invalid, check directly
        if not dataset_exists:
            dataset_exists = self.resource_exists(dataset_endpoint)
            if dataset_exists:
                href = dataset_endpoint
        
        # Handle according to update strategy
        if dataset_exists:
            if update_strategy == 'create_only':
                raise ValueError(f"Dataset '{dataset.title}' already exists and update_strategy is 'create_only'")
            
            if update_strategy in ['update_only', 'create_or_update']:
                # Update the existing dataset
                response = self.update_resource(
                    endpoint=href or dataset_endpoint,
                    data=dataset.to_dict(),
                    replace=force_replace
                )
                
                # Ensure the mapping is updated
                if ods_id and response.get('href') and response.get('uuid'):
                    self.mapping.add_entry(ods_id, response['uuid'], response['href'])
                    
                return response
        else:
            if update_strategy == 'update_only':
                raise ValueError(f"Dataset '{dataset.title}' does not exist and update_strategy is 'update_only'")
            
            if update_strategy in ['create_only', 'create_or_update']:
                # Create a new dataset
                response = self.create_resource(
                    endpoint=collection_endpoint,
                    data=dataset.to_dict()
                )
                
                # Store the mapping for future reference
                if ods_id and response.get('href') and response.get('uuid'):
                    self.mapping.add_entry(ods_id, response['uuid'], response['href'])
                    
                return response
        
        # This should not happen if the code is correct
        raise RuntimeError("Unexpected error in create_or_update_dataset")

    # TODO: Implement me
    def delete_dataset(self, title: str, fail_if_not_exists: bool = False) -> bool:
        """
        Delete a dataset from the DNK.
        
        Args:
            title (str): The title/name of the dataset to delete
            fail_if_not_exists (bool): Whether to raise an error if the dataset doesn't exist
            
        Returns:
            bool: True if the dataset was deleted, False if it didn't exist
            
        Raises:
            ValueError: If the dataset doesn't exist and fail_if_not_exists is True
            HTTPError: If API requests fail
        """
        pass
    
    # TODO: Do NOT implement me
    def build_organization_hierarchy_from_ods(self, org_data: Dict[str, Any]):
        """
        Build organization hierarchy in the DNK based on organization data from ODS API.
        
        Args:
            org_data (Dict[str, Any]): Dictionary containing organization data from ODS API
            
        Raises:
            ValueError: If organization data is missing or invalid
        """
        
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
                url_website = generate_potential_staatskalender_url(title_full)

                # Increment the counter
                current_org += 1

                # Check if the generated url is valid
                try:
                    response = requests_get(url_website)
                    if not response.status_code == 200:
                        logging.warning(f"Invalid URL for organization '{title}': {url_website}.")
                        url_website = org.get('url_website', '')
                        logging.info(f"Invalid URL for organization '{title}', falling back to {url_website}")
                except Exception as e: # TODO (large language model): Specify Exception
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
                    # TODO: Implement this (also don't forget to "Mark as processed" by processed_orgs.add(org_id)
                    # TODO: Actually build the structure based on path_components. Create Collections of stereotype "Organisationseinheiten" only.
                    pass


                    
                except Exception as e:
                    logging.error(f"Error processing organization {org_id}: {str(e)}")
                    failed_orgs.add(org_id)
        
        # Log summary statistics
        logging.info(f"Processed {len(processed_orgs)} organizations successfully")
        if failed_orgs:
            logging.warning(f"Failed to process {len(failed_orgs)} organizations")
