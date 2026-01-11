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
    
    mapping: Any  # InMemoryMapping or subclass

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
    
    def _download_and_update_mappings(self, target_ids: List[str] = None) -> int:
        """
        Base method for downloading assets and updating mappings.
        
        Args:
            target_ids: If provided, only update mappings for these IDs
            
        Returns:
            int: Number of mappings successfully updated
        """
        if not self.asset_id_field:
            logging.error("asset_id_field not set in subclass")
            raise NotImplementedError("Subclasses must set asset_id_field")
        
        if not self.asset_type_filter:
            logging.error("asset_type_filter not set in subclass")
            raise NotImplementedError("Subclasses must set asset_type_filter")
        
        logging.info(f"Downloading assets from {self.scheme_name} scheme")
        
        # Get assets from the scheme, passing the filter to enable caching
        assets = self.client.get_all_assets_from_scheme(filter_function=self.asset_type_filter)
        
        if not assets:
            logging.warning(f"No assets found in {self.scheme_name}")
            return 0
        
        # Create a lookup dictionary for faster access
        asset_by_id = {}
        for asset in assets:
            # Get the ID field value
            id_value = asset.get(self.asset_id_field)
            if id_value is not None:  # Check if the ID exists
                # Ensure the key is always a string
                id_str = str(id_value)
                asset_by_id[id_str] = asset
            else:
                # Log if an asset is missing the ID
                logging.debug(f"Found asset missing '{self.asset_id_field}': {asset.get('id', 'Unknown ID')}")
        
        # Process each asset and update the mapping
        updated_count = 0
        
        # Check for assets in mapping that are not in downloaded assets
        if not target_ids:
            removed_count = 0
            for id_value, entry in list(self.mapping.mapping.items()):
                if id_value not in asset_by_id:
                    logging.warning(f"Asset {id_value} exists in local mapping but not in dataspot. Removing from local mapping.")
                    if len(entry) >= 2:
                        _type, uuid = entry[0], entry[1]
                        logging.debug(f"    - _type: {_type}, uuid: {uuid}")
                    self.mapping.remove_entry(id_value)
                    removed_count += 1
            if removed_count > 0:
                logging.info(f"Found {removed_count} assets that exist locally but not in dataspot.")
        
        # If we have target IDs, prioritize those
        if target_ids and len(target_ids) > 0:
            total_targets = len(target_ids)
            target_ids.sort()
            for idx, id_value in enumerate(target_ids, 1):
                logging.debug(f"[{idx}/{total_targets}] Processing asset with ID: {id_value}")
                
                # Find this ID in our downloaded assets
                asset = asset_by_id.get(id_value)
                if not asset:
                    logging.warning(f"Could not find asset with ID {id_value} in downloaded data, skipping")
                    continue
                
                # Get the UUID and _type
                uuid = asset.get('id')
                _type = asset.get('_type')
                if not uuid:
                    logging.warning(f"Asset with ID {id_value} missing UUID, skipping")
                    continue
                if not _type:
                    logging.warning(f"Asset with ID {id_value} missing _type, skipping")
                    continue
                
                # Extract inCollection business key directly from the downloaded asset
                inCollection_key = asset.get('inCollection')
                
                if uuid and _type:
                    # Check if the mapping has changed before updating
                    existing_entry = self.mapping.get_entry(id_value)
                    # Existing entry tuple: (_type, uuid, inCollection)
                    if (existing_entry and
                            existing_entry[0] == _type and
                            existing_entry[1] == uuid and
                            existing_entry[2] == inCollection_key):
                        logging.debug(f"No changes in asset {id_value}. Skipping")
                        logging.debug(f"    - _type: {_type}, uuid: {uuid}, inCollection: {inCollection_key}")
                    elif existing_entry:
                        old_type = existing_entry[0] if len(existing_entry) > 0 else None
                        old_uuid = existing_entry[1] if len(existing_entry) > 1 else None
                        old_inCollection = existing_entry[2] if len(existing_entry) > 2 else None

                        # Only log UUID update warning if the UUID actually changed
                        if old_uuid != uuid:
                            logging.warning(f"Update asset {id_value} uuid from {old_uuid} to {uuid}")
                        else:
                            logging.info(f"Updating asset {id_value} metadata")

                        # Log changes in type if they occur
                        if old_type != _type:
                            logging.warning(f"Asset {id_value} type changed from '{old_type}' to '{_type}'")

                        # Log a more meaningful message if inCollection has changed
                        if old_inCollection != inCollection_key and old_inCollection and inCollection_key:
                            logging.info(f"Asset {id_value} has been moved from '{old_inCollection}' to '{inCollection_key}'")
                        elif not old_inCollection and inCollection_key:
                            logging.info(f"Asset {id_value} has been placed in '{inCollection_key}'")
                        elif old_inCollection and not inCollection_key:
                            logging.info(f"Asset {id_value} has been removed from '{old_inCollection}'")
                        
                        logging.debug(f"    - old_type: {old_type}, old_uuid: {old_uuid}, old_inCollection: {old_inCollection}")
                        logging.debug(f"    - new_type: {_type}, new_uuid: {uuid}, new_inCollection: {inCollection_key}")
                        self.mapping.add_entry(id_value, _type, uuid, inCollection_key)
                        updated_count += 1
                    else:
                        logging.debug(f"Add asset {id_value} with uuid {uuid}")
                        logging.debug(f"    - _type: {_type}, uuid: {uuid}, inCollection: {inCollection_key}")
                        self.mapping.add_entry(id_value, _type, uuid, inCollection_key)
                        updated_count += 1
                else:
                    logging.warning(f"Missing UUID or _type for asset with ID: {id_value}")
        else:
            # No target IDs, process all assets
            total_assets = len(assets)
            for idx, asset in enumerate(assets, 1):
                # Extract the ID from the asset
                id_value_raw = asset.get(self.asset_id_field)
                
                # Skip if this asset doesn't have the required ID
                if id_value_raw is None:
                    continue
                
                # Ensure we use the string version for lookup/processing
                id_value = str(id_value_raw)
                
                logging.debug(f"[{idx}/{total_assets}] Processing asset with ID: {id_value}")
                
                # Get the UUID and _type
                uuid = asset.get('id')
                _type = asset.get('_type')
                if not uuid:
                    logging.warning(f"Asset with ID {id_value} missing UUID, skipping")
                    continue
                if not _type:
                    logging.warning(f"Asset with ID {id_value} missing _type, skipping")
                    continue

                # Extract inCollection business key directly from the downloaded asset
                inCollection_key = asset.get('inCollection')
                
                if uuid and _type:
                    # Check if the mapping has changed before updating
                    existing_entry = self.mapping.get_entry(id_value)
                    # Existing entry tuple: (_type, uuid, inCollection)
                    if (existing_entry and
                            existing_entry[0] == _type and
                            existing_entry[1] == uuid and
                            existing_entry[2] == inCollection_key):
                        logging.debug(f"No changes in asset {id_value}. Skipping")
                        logging.debug(f"    - _type: {_type}, uuid: {uuid}, inCollection: {inCollection_key}")
                    elif existing_entry:
                        old_type = existing_entry[0] if len(existing_entry) > 0 else None
                        old_uuid = existing_entry[1] if len(existing_entry) > 1 else None
                        old_inCollection = existing_entry[2] if len(existing_entry) > 2 else None

                        # Only log UUID update warning if the UUID actually changed
                        if old_uuid != uuid:
                            logging.warning(f"Update asset {id_value} uuid from {old_uuid} to {uuid}")
                        else:
                            logging.info(f"Updating asset {id_value} metadata")

                        # Log changes in type if they occur
                        if old_type != _type:
                            logging.warning(f"Asset {id_value} type changed from '{old_type}' to '{_type}'")

                        # Log a more meaningful message if inCollection has changed
                        if old_inCollection != inCollection_key and old_inCollection and inCollection_key:
                            logging.info(f"Asset {id_value} has been moved from '{old_inCollection}' to '{inCollection_key}'")
                        elif not old_inCollection and inCollection_key:
                            logging.info(f"Asset {id_value} has been placed in '{inCollection_key}'")
                        elif old_inCollection and not inCollection_key:
                            logging.info(f"Asset {id_value} has been removed from '{old_inCollection}'")
                        
                        self.mapping.add_entry(id_value, _type, uuid, inCollection_key)
                        updated_count += 1
                    else:
                        logging.debug(f"Add asset {id_value} with uuid {uuid}")
                        self.mapping.add_entry(id_value, _type, uuid, inCollection_key)
                        updated_count += 1
                else:
                    logging.warning(f"Missing UUID or _type for asset with ID: {id_value}")
        
        logging.info(f"Updated mappings for {updated_count} assets. Did not update mappings for the other {len(assets) - updated_count} assets.")
        return updated_count
    
    def update_mappings_before_upload(self) -> int:
        """
        Updates mappings from Dataspot before uploading any assets.
        Ensures we have the latest mapping data for proper updates.
        
        Returns:
            int: Number of mappings successfully updated
        """
        logging.info("Updating mappings from Dataspot before upload")
        
        updated_count = self._download_and_update_mappings()
        logging.info(f"Updated {updated_count} mappings before upload")
        return updated_count
    
    def update_mappings_after_upload(self, ids: List[str]) -> None:
        """
        Updates the mapping between external IDs and Dataspot UUIDs after uploading assets.
        
        Args:
            ids: List of external IDs to update in the mapping
            
        Raises:
            HTTPError: If API requests fail
            ValueError: If unable to retrieve asset information
        """
        logging.info(f"Updating mappings for {len(ids)} assets using download API")
        
        updated_count = self._download_and_update_mappings(ids)
        logging.info(f"Updated mappings for {updated_count} out of {len(ids)} assets")

        # FIXME: I think this is buggy (we should not compare the number, but the actual ids, right??)
        if updated_count < len(ids):
            missing_ids = [id_value for id_value in ids if not self.mapping.get_entry(id_value)]
            if missing_ids:
                logging.warning(f"Could not find mappings for {len(missing_ids)} IDs: {missing_ids[:5]}" + 
                                  (f"... and {len(missing_ids)-5} more" if len(missing_ids) > 5 else ""))
    
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
