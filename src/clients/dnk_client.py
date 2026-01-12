from typing import List, Dict, Any
import logging

import config
from src.clients.base_client import BaseDataspotClient
from src.clients.helpers import strip_quotes
from src.mapping_handlers.org_structure_handler import OrgStructureHandler
from src.mapping_handlers.dataset_handler import DatasetHandler
from src.dataspot_dataset import Dataset

class DNKClient(BaseDataspotClient):
    """Client for interacting with the DNK (Datennutzungskatalog)."""
    
    def __init__(self):
        """
        Initialize the DNK client.
        """
        super().__init__(scheme_name=config.dnk_scheme_name,
                         scheme_name_short=config.dnk_scheme_name_short,
                         ods_imports_collection_name=config.dnk_ods_imports_collection_name,
                         ods_imports_collection_path=config.dnk_ods_imports_collection_path)
        
        # Initialize cache for Dataset objects
        self._datasets_cache = None
        
        # Initialize the handlers
        self.org_handler = OrgStructureHandler(self)
        self.dataset_handler = DatasetHandler(self)

    def get_datasets_with_cache(self) -> List[Dict[str, Any]]:
        """
        Get Dataset objects with caching support.
        
        Uses SQL Query API to fetch only the required assets with all filtering done in the query.
        No in-memory filtering is needed.
            
        Returns:
            List[Dict[str, Any]]: List of Dataset assets
        """
        # Check if cache is populated
        if self._datasets_cache is not None:
            logging.info(f"Using cached Datasets from {self.scheme_name_short} scheme ({len(self._datasets_cache)} assets)")
            return list(self._datasets_cache)
        
        # Cache is empty, fetch using SQL Query API
        logging.info(f"Fetching Dataset assets from {self.scheme_name_short} scheme using SQL Query API")
        
        query = """
            SELECT 
                d.id,
                d._type,
                d.in_collection,
                d.label,
                d.stereotype,
                d.status,
                cp.value AS ods_dataportal_id
            FROM 
                dataset_view d
            JOIN
                customproperties_view cp ON d.id = cp.resource_id AND cp.name = 'odsDataportalId'
            WHERE 
                d._type = 'Dataset'
                AND cp.value IS NOT NULL
                AND d.status NOT IN ('INTERMINATION2', 'ARCHIVEMETA')
            ORDER BY
                d.label
        """
        
        results = self.execute_query_api(sql_query=query)
        
        # Convert SQL results to match Download API format (snake_case to camelCase)
        self._datasets_cache = []
        for row in results:
            asset = {
                'id': row.get('id'),
                '_type': row.get('_type'),
                'inCollection': row.get('in_collection'),
                'label': row.get('label'),
                'stereotype': row.get('stereotype'),
                'status': row.get('status'),
                'odsDataportalId': strip_quotes(row.get('ods_dataportal_id'))
            }
            self._datasets_cache.append(asset)
        
        logging.info(f"Cached {len(self._datasets_cache)} Datasets from {self.scheme_name_short} scheme")
        
        return list(self._datasets_cache)

    def clear_datasets_cache(self) -> None:
        """
        Clear the Datasets cache, forcing a fresh download on the next request.
        
        Call this method if you know the Dataset data has changed externally
        or after making changes to Dataset objects.
        """
        self._datasets_cache = None
        logging.info(f"Cleared Datasets cache for {self.scheme_name_short} scheme")

    def get_datasets_cache(self) -> List[Dict[str, Any]] | None:
        """
        Get the current Datasets cache for debugging/inspection.
        
        Returns:
            List[Dict[str, Any]] | None: The cached Datasets, or None if cache is not populated
        """
        return self._datasets_cache

    # Direct API operations for datasets
    def create_dataset(self, dataset: Dataset, status: str = "WORKING") -> dict:
        """
        Create a new dataset in the location specified by config.ods_imports_collection_path
        
        Args:
            dataset: The dataset instance to be uploaded
            status: Status to set on the dataset. Defaults to "WORKING" (DRAFT group).
                   Set to None to use the default status for datasets.
            
        Returns:
            dict: The JSON response containing the dataset data
        """
        # Create dataset endpoint
        collection_uuid = self._ods_imports_collection.get('id')
        if not collection_uuid:
            raise ValueError("Failed to get collection UUID")
            
        # Prepare dataset for upload with proper inCollection value
        dataset_json = dataset.to_json()
        # Use the collection_name from our configuration as the inCollection value
        dataset_json['inCollection'] = self.ods_imports_collection_name
        
        # Create the dataset directly
        endpoint = f"/rest/{config.database_name}/collections/{collection_uuid}/datasets"
        return self._create_asset(endpoint=endpoint, data=dataset_json, status=status)
    
    def update_dataset(self, dataset: Dataset, uuid: str, force_replace: bool = False, status: str = "WORKING") -> dict:
        """
        Update an existing dataset in the DNK.
        
        Args:
            dataset: The dataset instance with updated data
            uuid: The UUID of the dataset to update
            force_replace: Whether to completely replace the dataset
            status: Status to set on the dataset. Defaults to "WORKING" (DRAFT group).
                   Set to None to preserve the dataset's current status.
            
        Returns:
            dict: The JSON response containing the updated dataset data
        """
        endpoint = f"/rest/{config.database_name}/datasets/{uuid}"
        return self._update_asset(endpoint=endpoint, data=dataset.to_json(), replace=force_replace, status=status)
    
    def mark_dataset_for_deletion(self, uuid: str) -> bool:
        """
        Mark a dataset for deletion review in Dataspot.
        This sets the dataset's status to "DELETENEW" for later review.
        
        Args:
            uuid (str): The UUID of the dataset to mark for deletion
            
        Returns:
            bool: True if the dataset was marked successfully, False if it doesn't exist
        """
        endpoint = f"/rest/{config.database_name}/datasets/{uuid}"
        try:
            # Check if asset exists first
            if self._get_asset(endpoint) is None:
                logging.warning(f"Dataset with UUID {uuid} not found in Dataspot, cannot mark for deletion")
                return False
                
            self._mark_asset_for_deletion(endpoint)
            return True
        except Exception as e:
            logging.error(f"Error marking dataset with UUID {uuid} for deletion: {str(e)}")
            return False

    def bulk_create_or_update_datasets(self, datasets: List[Dataset],
                                      operation: str = "ADD", dry_run: bool = False,
                                      status: str = "WORKING") -> dict:
        """
        Create multiple datasets in bulk in Dataspot.
        
        Args:
            datasets: List of dataset instances to be uploaded
            operation: Upload operation mode (ADD, REPLACE, FULL_LOAD)
            dry_run: Whether to perform a test run without changing data
            status: Status to set on all datasets in the upload. Defaults to "WORKING" (DRAFT group).
                   Set to None to preserve existing statuses or use defaults.
            
        Returns:
            dict: The JSON response containing the upload results
        """
        # Create full path to the ODS-Imports collection
        full_collection_path = f"{'/'.join(self.ods_imports_collection_path)}/{self.ods_imports_collection_name}"
        
        dataset_jsons = [dataset.to_json() for dataset in datasets]
        
        # Set inCollection for each dataset using the full path
        for dataset_json in dataset_jsons:
            dataset_json['inCollection'] = full_collection_path
            
        return self.bulk_create_or_update_assets(
            scheme_name=self.scheme_name,
            data=dataset_jsons,
            operation=operation,
            dry_run=dry_run,
            status=status
        )
