import logging
from typing import List, Dict, Any

from src.clients.base_client import BaseDataspotClient


class BaseDataspotHandler:
    """
    Base class for Dataspot handlers that manage different types of assets.
    This class provides common functionality for dataset and organizational unit handlers.
    """
    # Configuration values to be set by subclasses
    asset_id_field = None  # Field name for the external ID (e.g., 'odsDataportalId', 'stateCalendarId')
    asset_type_filter = None  # Filter function or criteria for asset type

    def __init__(self, client: BaseDataspotClient):
        """
        Initialize the base handler.
        
        Args:
            client: BaseDataspotClient instance to use for API operations
        """
        self.client = client
        
        # Load common properties from client
        self.database_name = client.database_name
        self.scheme_name = client.scheme_name
        self.scheme_name_short = client.scheme_name_short
    
    def bulk_create_or_update_assets(self, assets: List[Dict[str, Any]], 
                                     operation: str = "ADD", dry_run: bool = False,
                                     status: str = "WORKING") -> dict:
        """
        Create multiple assets in bulk in Dataspot.
        
        Args:
            assets: List of asset data to upload
            operation: Upload operation mode (ADD, REPLACE, FULL_LOAD)
            dry_run: Whether to perform a test run without changing data
            status: Status to set on all assets in the upload. Defaults to "WORKING" (DRAFT group).
                   Set to None to preserve existing statuses or use defaults.
            
        Returns:
            dict: The JSON response from the API containing the upload results
            
        Raises:
            ValueError: If no assets are provided
            HTTPError: If API requests fail
        """
        # Verify we have assets to process
        if not assets:
            logging.warning("No assets provided for bulk upload")
            return {"status": "error", "message": "No assets provided"}
        
        # Count of assets
        num_assets = len(assets)
        logging.info(f"Bulk creating {num_assets} assets with status '{status}' (operation: {operation}, dry_run: {dry_run})...")
        
        # Bulk create assets using the scheme name
        try:
            response = self.client.bulk_create_or_update_assets(
                scheme_name=self.scheme_name,
                data=assets,
                operation=operation,
                dry_run=dry_run,
                status=status
            )

            logging.info(f"Bulk creation complete")
            return response
            
        except Exception as e:
            logging.error(f"Unexpected error during bulk upload: {str(e)}")
            raise
