import ods_utils_py as ods_utils
import logging
import os
import time
from typing import Dict, List, Any

class ODSClient:
    """Client for interacting with the ODS API."""

    def __init__(self):
        """
        Initialize the ODS client.

        """
        self.explore_api_version = 'v2.1'

    def get_dataset_columns(self, dataset_id: str) -> list:
        """
        Get the list of columns/fields for a dataset. Also strips leading and trailing white spaces from label and name.
        
        Args:
            dataset_id (str, optional): The dataset ID. Defaults to None.
            
        Returns:
            list: List of column configurations for the dataset
            
        Raises:
            SystemExit: If neither nor both dataset_id and dataset_uid are provided
            HTTPError: If the API request fails
        """
        if not dataset_id:
            exit("Error: dataset_id has to be specified.")
        
        # Use the Explore API v2.1 to get dataset information
        r = ods_utils.requests_get(url=f"https://data.bs.ch/api/explore/{self.explore_api_version}/catalog/datasets/{dataset_id}")
        r.raise_for_status()

        # Extract the field configurations from the response
        dataset_info = r.json()
        fields = dataset_info.get('fields', [])

        column_info = []
        for field in fields:
            column_data = {
                'label': field.get('label').strip() if field.get('label') else None,
                'name': field.get('name').strip() if field.get('name') else None,
                'type': field.get('type').strip() if field.get('type') else None,
                'description': None if field.get('description') == '' else field.get('description').strip() if field.get('description') else field.get('description'),
            }
            if field.get('type') == 'text' and 'semantic_type' in field and field.get('semantic_type') == 'identifier':
                column_data['type'] = 'identifier'

            column_info.append(column_data)

        return column_info

    def _get_organization_data_batch(self, limit: int = 100, offset: int = 0) -> dict:
        """
        Get organization data from a specific dataset using the Explore API.
        
        Args:
            limit (int): Maximum number of records to return. Defaults to 100.
            offset (int): Offset to start from. Defaults to 0.
            
        Returns:
            dict: JSON response containing organization data
            
        Raises:
            HTTPError: If the API request fails
        """
        # Construct the API URL with query parameters
        url = f"https://data.bs.ch/api/explore/{self.explore_api_version}/catalog/datasets/100349/records"
        
        params = {
            "select": "id,title,parent_id,html,children_id",
            "order_by": "id",
            "limit": limit,
            "offset": offset
        }
        
        # Make the request
        response = ods_utils.requests_get(url=url, params=params)
        response.raise_for_status()
        
        # Return the JSON response
        return response.json()

    def get_all_organization_data(self, batch_size: int = 100, max_batches: int = None) -> dict:
        """
        Get all organization data from the Staatskalender dataset by retrieving multiple batches.
        
        Args:
            batch_size (int): Number of records to retrieve in each batch. Defaults to 100.
            max_batches (int, optional): Maximum number of batches to retrieve. 
                                        If None, retrieves all available data.
            
        Returns:
            dict: Combined JSON response containing all organization data with a 'results' list
            
        Raises:
            HTTPError: If any API request fails
        """
        logging.info("Fetching all organization data from ODS API...")
        all_organizations: Dict[str, List[Any]] = {"results": []}
        batch_count = 0
        total_retrieved = 0
        
        while True:
            # Get the next batch of organization data
            offset = batch_count * batch_size
            batch_data = self._get_organization_data_batch(limit=batch_size, offset=offset)
            
            # Check if we received any results
            batch_results = batch_data.get('results', [])
            num_results = len(batch_results)
            
            if num_results == 0:
                # No more results, break out of the loop
                break
            
            # Add the batch results to our collected data
            all_organizations['results'].extend(batch_results)
            total_retrieved += num_results
            
            logging.info(f"Retrieved batch {batch_count + 1} with {num_results} organizations (total: {total_retrieved})")
            
            # Check if we've reached our batch limit
            batch_count += 1
            if max_batches is not None and batch_count >= max_batches:
                logging.info(f"Reached the maximum number of batches ({max_batches})")
                break
        
        # Set the total count in the combined data
        all_organizations['total_count'] = batch_data.get('total_count', total_retrieved)
        logging.info(f"Total organizations retrieved: {total_retrieved} (out of {all_organizations['total_count']})")
        
        return all_organizations

    def get_all_dataset_ids_with_restricted_flag(self, max_datasets: int = None, cooldown: float = 1.0) -> List[Dict[str, Any]]:
        """
        Retrieve all ODS dataset ids together with their is_restricted flag, regardless of restriction status.

        This mirrors the pagination logic of ods_utils_py's get_all_dataset_ids(), but keeps the
        is_restricted flag per dataset instead of discarding it, since callers need to distinguish
        restricted from unrestricted datasets in the same listing. Requires an API key with permission
        to see restricted datasets.

        Args:
            max_datasets (int, optional): Maximum number of dataset entries to return. If None, all
                datasets are returned.
            cooldown (float): Sleep time in seconds between requests to avoid overloading the server.
                Defaults to 1.0.

        Returns:
            List[Dict[str, Any]]: A list of {'dataset_id': str, 'is_restricted': bool} dicts, one per
                ODS dataset.

        Raises:
            HTTPError: If any API request fails
        """
        ods_domain = os.getenv('ODS_DOMAIN')
        ods_api_type = os.getenv('ODS_API_TYPE')
        base_url = "https://" + f"{ods_domain}/api/{ods_api_type}".replace("//", "/")

        batch_size = 100
        r = ods_utils.requests_get(url=f"{base_url}/datasets/?limit={batch_size}")
        r.raise_for_status()

        all_datasets: List[Dict[str, Any]] = []

        while True:
            all_datasets += [
                {'dataset_id': item['dataset_id'], 'is_restricted': item['is_restricted']}
                for item in r.json().get('results', {})
            ]

            # Check if we have reached the maximum number of datasets
            if max_datasets is not None and len(all_datasets) >= max_datasets:
                all_datasets = all_datasets[:max_datasets]
                break

            next_request_url = r.json().get('next', None)

            if not next_request_url:
                break

            # Add cooldown between requests
            time.sleep(cooldown)

            r = ods_utils.requests_get(url=next_request_url)
            r.raise_for_status()

        all_datasets.sort(key=lambda d: d['dataset_id'])

        logging.info(f"Retrieved {len(all_datasets)} ODS dataset ids (with restricted flag)")
        return all_datasets
