from typing import Dict, Any

import config
from src.clients.base_client import BaseDataspotClient
from src.mapping_handlers.org_structure_handler import OrgStructureHandler

class RDMClient(BaseDataspotClient):
    """Client for interacting with your new data scheme."""
    
    def __init__(self):
        """
        Initialize the new client.
        """
        super().__init__(base_url=config.base_url,
                         database_name=config.database_name,
                         scheme_name=config.rdm_scheme_name,
                         scheme_name_short=config.rdm_scheme_name_short,
                         ods_imports_collection_name=config.ods_imports_collection_name,
                         ods_imports_collection_path=config.ods_imports_collection_path)
        
        # Initialize the handlers
        self.org_handler = OrgStructureHandler(self)

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
