import logging
from typing import Dict, Any, List, Optional

import config
from src.clients.base_client import BaseDataspotClient
from src.mapping_handlers.org_structure_handler import OrgStructureHandler
from src.mapping_handlers.dataset_component_handler import DatasetComponentHandler
from src.ods_client import ODSClient

class TDMClient(BaseDataspotClient):
    """Client for interacting with your new data scheme."""
    
    def __init__(self):
        """
        Initialize the new client.
        """
        super().__init__(base_url=config.base_url,
                         database_name=config.database_name,
                         scheme_name=config.tdm_scheme_name,
                         scheme_name_short=config.tdm_scheme_name_short,
                         ods_imports_collection_name=config.tdm_ods_imports_collection_name,
                         ods_imports_collection_path=config.tdm_ods_imports_collection_path)
        
        # Initialize the handlers
        self.org_handler = OrgStructureHandler(self)
        self.component_handler = DatasetComponentHandler(self)

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
        
    def sync_dataset_components(self, ods_id: str, name: str, columns: List[Dict[str, Any]], title: Optional[str] = None) -> Dict[str, Any]:
        """
        Create or update a TDM dataobject for a dataset with its columns as attributes.
        
        Args:
            ods_id (str): The ODS ID of the dataset
            name (str): The name of the dataset/dataobject (used as label)
            columns (List[Dict[str, Any]]): List of column definitions, each containing:
                - label: Human-readable label
                - name: Technical column name
                - type: Data type of the column
                - description: Description of the column
            title (str, optional): The title of the dataset/dataobject. If provided, will be set as the "title" field.
                
        Returns:
            Dict[str, Any]: Result of the operation with status and details
        """
        return self.component_handler.sync_dataset_components(ods_id, name, columns, title)
