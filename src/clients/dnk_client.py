from typing import Dict, Any, List
import logging

import config
from src.clients.base_client import BaseDataspotClient
from src.mapping_handlers.org_structure_handler import OrgStructureHandler
from src.mapping_handlers.dataset_handler import DatasetHandler
from src.dataspot_dataset import Dataset

class DNKClient(BaseDataspotClient):
    """Client for interacting with the DNK (Datennutzungskatalog)."""
    
    def __init__(self):
        """
        Initialize the DNK client.
        """
        super().__init__(base_url=config.base_url,
                         database_name=config.database_name,
                         scheme_name=config.dnk_scheme_name,
                         scheme_name_short=config.dnk_scheme_name_short,
                         ods_imports_collection_name=config.dnk_ods_imports_collection_name,
                         ods_imports_collection_path=config.dnk_ods_imports_collection_path)
        
        # Initialize the handlers
        self.org_handler = OrgStructureHandler(self)
        self.dataset_handler = DatasetHandler(self)

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
        # Ensure ODS-Imports collection exists
        collection_data = self.ensure_ods_imports_collection_exists()
        
        # Create dataset endpoint
        collection_uuid = collection_data.get('id')
        if not collection_uuid:
            raise ValueError("Failed to get collection UUID")
            
        # Prepare dataset for upload with proper inCollection value
        dataset_json = dataset.to_json()
        # Use the collection_name from our configuration as the inCollection value
        dataset_json['inCollection'] = self.ods_imports_collection_name
        
        # Create the dataset directly
        endpoint = f"/rest/{self.database_name}/collections/{collection_uuid}/datasets"
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
        endpoint = f"/rest/{self.database_name}/datasets/{uuid}"
        return self._update_asset(endpoint=endpoint, data=dataset.to_json(), replace=force_replace, status=status)
    
    def delete_dataset(self, ods_id: str, fail_if_not_exists: bool = False) -> bool:
        """
        Delete a dataset from the DNK by marking it for deletion review,
        or update local tracking if already deleted from Dataspot.
        
        This is a higher-level method that handles checking for existence,
        marking for deletion, and updating the mapping.
        
        Args:
            ods_id (str): The ODS ID of the dataset to delete
            fail_if_not_exists (bool): Whether to raise an error if the dataset doesn't exist
            
        Returns:
            bool: True if the dataset was deleted or marked for deletion, or if it didn't exist but tracking was updated.
                 False if it didn't exist in mapping and fail_if_not_exists is False.
        """
        return self.dataset_handler.delete_dataset(ods_id, fail_if_not_exists)
    
    def mark_dataset_for_deletion(self, uuid: str) -> bool:
        """
        Mark a dataset for deletion review in Dataspot.
        This sets the dataset's status to "REVIEWDCC2" for later review.
        
        Args:
            uuid (str): The UUID of the dataset to mark for deletion
            
        Returns:
            bool: True if the dataset was marked successfully, False if it doesn't exist
        """
        endpoint = f"/rest/{self.database_name}/datasets/{uuid}"
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
    
    # Synchronization methods
    def sync_org_units(self, org_data: Dict[str, Any], status: str = "WORKING") -> Dict[str, Any]:
        """
        Synchronize organizational units in Dataspot with data from the Staatskalender ODS API.
        
        Args:
            org_data: Dictionary containing organization data from ODS API
            status: Status to set on updated org units. Defaults to "WORKING" (DRAFT group).
                   Use "PUBLISHED" to make updates public immediately.
            
        Returns:
            Dict: Summary of the synchronization process
        """
        return self.org_handler.sync_org_units(org_data, status=status)
    
    def sync_datasets(self, datasets: List[Dataset]) -> Dict[str, Any]:
        """
        Synchronize datasets between ODS and Dataspot.
        
        Args:
            datasets: List of Dataset objects to synchronize
            
        Returns:
            Dict: Report of the synchronization process
        """
        return self.dataset_handler.sync_datasets(datasets)
