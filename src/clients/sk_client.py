import config
from src.clients.base_client import BaseDataspotClient
from src.mapping_handlers.org_structure_handler import OrgStructureHandler

class SKClient(BaseDataspotClient):
    """Client for interacting with the 'Systemkatalog' scheme."""
    
    def __init__(self):
        """
        Initialize the new client.
        """
        super().__init__(scheme_name=config.sk_scheme_name,
                         scheme_name_short=config.sk_scheme_name_short)
        
        # Initialize the handlers
        self.org_handler = OrgStructureHandler(self)
