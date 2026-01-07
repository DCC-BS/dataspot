import config
from src.clients.base_client import BaseDataspotClient
from src.mapping_handlers.org_structure_handler import OrgStructureHandler
from src.mapping_handlers.dataset_composition_handler import DatasetCompositionHandler

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
        self.composition_handler = DatasetCompositionHandler(self)
