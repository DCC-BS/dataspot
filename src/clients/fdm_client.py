import config
from src.clients.base_client import BaseDataspotClient
from src.mapping_handlers.org_structure_handler import OrgStructureHandler

class FDMClient(BaseDataspotClient):
    """Client for interacting with your new data scheme."""
    
    def __init__(self):
        """
        Initialize the new client.
        """
        super().__init__(scheme_name=config.fdm_scheme_name,
                         scheme_name_short=config.fdm_scheme_name_short)
        
        # Initialize the handlers
        self.org_handler = OrgStructureHandler(self)
