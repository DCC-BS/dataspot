import logging
from typing import Dict, Any, List, Optional

from src.clients.base_client import BaseDataspotClient
from src.mapping_handlers.base_dataspot_handler import BaseDataspotHandler
from src.mapping_handlers.in_memory_mapping import InMemoryMapping
from src.mapping_handlers.org_structure_helpers.org_structure_transformer import OrgStructureTransformer
from src.mapping_handlers.org_structure_helpers.org_structure_comparer import OrgStructureComparer
from src.mapping_handlers.org_structure_helpers.org_structure_updater import OrgStructureUpdater


class OrgStructureMapping(InMemoryMapping):
    """
    A lookup table that maps Staatskalender IDs to Dataspot asset type, UUID, and optionally inCollection.
    Fetches mappings from Dataspot API, stores in memory. Handles organizational units.
    """

    def __init__(self, database_name: str, scheme: str, client: BaseDataspotClient):
        """
        Initialize the mapping table for organizational units by fetching from Dataspot API.

        Args:
            database_name (str): Name of the database
            scheme (str): Name of the scheme (e.g., 'DNK', 'TDM')
            client (BaseDataspotClient): Client instance to use for API operations
        """
        # Filter function: Collection with stereotype='organizationalUnit' and stateCalendarId field
        filter_function = lambda asset: (
            asset.get('_type') == 'Collection' and 
            asset.get('stereotype') == 'organizationalUnit' and
            asset.get('stateCalendarId') is not None
        )
        super().__init__(database_name, "staatskalender_id", scheme, client, filter_function)


class OrgStructureHandler(BaseDataspotHandler):
    """
    Handler for organizational structure operations in Dataspot.
    Coordinates the transformation, comparison, and updating of organizational units.
    """
    
    # Set configuration values for the base handler
    asset_id_field = 'stateCalendarId'
    
    def __init__(self, client: BaseDataspotClient):
        """
        Initialize the OrgStructureHandler.
        
        Args:
            client: BaseDataspotClient instance to use for API operations
        """
        # Call parent's __init__ method first
        super().__init__(client)
        
        # Initialize the organization mapping (fetches from Dataspot API)
        self.mapping = OrgStructureMapping(database_name=client.database_name, scheme=client.scheme_name_short, client=client)
        
        # Set the asset type filter based on asset_id_field and stereotype
        self.asset_type_filter = lambda asset: (
            asset.get('_type') == 'Collection' and 
            asset.get('stereotype') == 'organizationalUnit' and
            asset.get(self.asset_id_field) is not None
        )
        
        # Create component instances
        self.updater = OrgStructureUpdater(client)
    
    def bulk_create_or_update_organizational_units(
        self, 
        organizational_units: List[Dict[str, Any]], 
        operation: str = "ADD", 
        dry_run: bool = False,
        status: str = "WORKING"
    ) -> dict:
        """
        Create multiple organizational units in bulk in Dataspot.
        
        Args:
            organizational_units: List of organizational unit data to upload
            operation: Upload operation mode (ADD, REPLACE, FULL_LOAD)
            dry_run: Whether to perform a test run without changing data
            status: Status to set on all organizational units in the upload. Defaults to "WORKING" (DRAFT group).
                   Set to None to preserve existing statuses or use defaults.
            
        Returns:
            dict: The JSON response from the API containing the upload results
        """
        if not organizational_units:
            logging.warning("No organizational units provided for bulk upload")
            return {"status": "error", "message": "No organizational units provided"}
            
        # Call the base class method with our specific asset type
        return self.bulk_create_or_update_assets(organizational_units, operation, dry_run, status)

    @staticmethod
    def _check_for_duplicate_ids_in_ods_staatskalender_data(org_data: Dict[str, Any]) -> None:
        """
        Check for duplicate stateCalendarId values in organization data.
        Raises ValueError if duplicates are found.

        Args:
            org_data: Dictionary containing organization data from ODS API

        Raises:
            ValueError: If duplicate ID values are detected
        """
        org_results = org_data.get('results', [])
        ids = [str(org.get('id')) for org in org_results if 'id' in org]
        duplicate_ids = set([org_id for org_id in ids if ids.count(org_id) > 1])

        if duplicate_ids:
            # Get the details of the duplicates for better logging
            duplicates_info = []
            for dup_id in duplicate_ids:
                dup_orgs = [org for org in org_results if str(org.get('id')) == dup_id]
                for org in dup_orgs:
                    duplicates_info.append(f"ID: {dup_id}, Title: '{org.get('title', 'Unknown')}'")

            # Log detailed warning
            logging.warning(f"Duplicate stateCalendarId values detected: {len(duplicate_ids)} IDs appear multiple times")
            for info in duplicates_info:
                logging.warning(f"Duplicate organization: {info}")

            # Raise an error
            duplicate_details = '\n'.join(duplicates_info)
            error_msg = f"Duplicate stateCalendarId values detected. The following {len(duplicate_ids)} IDs appear multiple times:\n{duplicate_details}"
            raise ValueError(error_msg)

    def _initialize_org_hierarchy_from_ods(self, org_data: Dict[str, Any], status: str = "WORKING") -> dict:
        """
        Build organization hierarchy in the DNK based on data from ODS API using bulk upload. This function should only
        be called if the org hierarchy does not yet exist at all in the scheme.
        
        Args:
            org_data: Dictionary containing organization data from ODS API
            status: Status to set on created org units. Defaults to "WORKING" (DRAFT group).
                   Use "PUBLISHED" to make units public immediately.
            
        Returns:
            dict: The response with status information
        """
        logging.info(f"Building organization hierarchy using bulk upload with status '{status}'...")
        
        # Check for duplicate stateCalendarId values
        self._check_for_duplicate_ids_in_ods_staatskalender_data(org_data)
        
        # Transform organization data to layered structure
        units_by_depth = OrgStructureTransformer.transform_to_layered_structure(org_data)
        
        if not units_by_depth:
            logging.warning("No organizational units to upload")
            return {"status": "error", "message": "No organizational units to upload"}
        
        # Set status on all units and extract Staatskalender IDs for mapping updates
        staatskalender_ids = []
        for depth, units in units_by_depth.items():
            for unit in units:
                # Set the status on each unit
                unit["status"] = status
                
                staatskalender_id = unit.get("stateCalendarId")
                if staatskalender_id:
                    staatskalender_ids.append(staatskalender_id)
        
        # Track uploaded units to handle failures
        level_results = {}
        upload_errors = []
        
        # Process each depth level in order
        for depth in sorted(units_by_depth.keys()):
            # Sort units by staatskalender_id within this depth level
            level_units = sorted(units_by_depth[depth], key=lambda unit: str(unit.get("stateCalendarId", "")))
            if not level_units:
                logging.info(f"No units to upload at depth level {depth}, skipping")
                continue
                
            logging.info(f"Uploading {len(level_units)} organizational units at depth level {depth}...")
            
            try:
                # Perform bulk upload for this level
                response = self.bulk_create_or_update_organizational_units(
                    organizational_units=level_units,
                    operation="ADD",
                    dry_run=False,
                    status=status
                )
                
                # Store the result for this level
                level_results[depth] = response
                logging.info(f"Successfully uploaded {len(level_units)} units at depth level {depth}")
                
                # If this level has errors, log them but continue
                if isinstance(response, list):
                    errors = [msg for msg in response if isinstance(msg, dict) and msg.get('level') == 'ERROR']
                    if errors:
                        error_count = len(errors)
                        logging.warning(f"Upload for level {depth} completed with {error_count} errors")
                        
                        # Log up to 5 errors in detail
                        for i, error in enumerate(errors[:5]):
                            error_msg = error.get('message', str(error))
                            upload_errors.append(f"Level {depth} error: {error_msg}")
                            logging.warning(f"Level {depth} upload error {i+1}: {error_msg}")
                        
                        if error_count > 5:
                            logging.warning(f"... and {error_count - 5} more errors")
            except Exception as e:
                error_msg = f"Error uploading level {depth}: {str(e)}"
                logging.error(error_msg)
                upload_errors.append(error_msg)
        
        # Update mappings after upload
        self.update_mappings_after_upload(staatskalender_ids)
        
        # Determine overall result
        if upload_errors:
            logging.warning(f"Upload completed with {len(upload_errors)} errors")
            result = {
                "status": "partial", 
                "message": "Hierarchy build completed with errors",
                "errors": upload_errors[:10],  # First 10 errors
                "total_errors": len(upload_errors),
                "levels_processed": len(level_results)
            }
        else:
            result = {
                "status": "success",
                "message": "Hierarchy build completed successfully",
                "levels_processed": len(level_results)
            }
            
        return result
    
    def _check_for_duplicate_ids_in_dataspot(self, dataspot_units: List[Dict[str, Any]]) -> None:
        """
        Check for duplicate stateCalendarId values in existing Dataspot collections.
        Raises ValueError if duplicates are found.
        
        Args:
            dataspot_units: List of organizational units from Dataspot
        
        Raises:
            ValueError: If duplicate ID values are detected
        """
        # Count occurrences of each stateCalendarId
        id_counts = {}
        for unit in dataspot_units:
            unit_id = str(unit.get(self.asset_id_field))
            if unit_id:
                if unit_id not in id_counts:
                    id_counts[unit_id] = []
                id_counts[unit_id].append(unit)
        
        # Find all IDs that appear more than once
        duplicate_ids = {id: units for id, units in id_counts.items() if len(units) > 1}
        
        if duplicate_ids:
            # Get the details of the duplicates for better logging
            duplicates_info = []
            for dup_id, units in duplicate_ids.items():
                for unit in units:
                    title = unit.get('label', 'Unknown')
                    uuid = unit.get('id', 'Unknown')
                    path = unit.get('inCollection', '')
                    duplicates_info.append(f"ID: {dup_id}, Title: '{title}', UUID: {uuid}, Path: '{path}'")
            
            # Log detailed warning
            logging.warning(f"Duplicate stateCalendarId values detected in Dataspot: {len(duplicate_ids)} IDs appear multiple times")
            for info in duplicates_info:
                logging.warning(f"Duplicate organization in Dataspot: {info}")
            
            # Raise an error
            duplicate_details = '\n'.join(duplicates_info)
            error_msg = f"Duplicate stateCalendarId values detected in Dataspot. The following {len(duplicate_ids)} IDs appear multiple times:\n{duplicate_details}"
            raise ValueError(error_msg)

    def sync_org_units(self, org_data: Dict[str, Any], status: str = "WORKING") -> Dict[str, Any]:
        """
        Synchronize organizational units in Dataspot with data from the Staatskalender ODS API.
        
        Args:
            org_data: Dictionary containing organization data from ODS API
            status: Status to set on updated org units. Defaults to "WORKING" (DRAFT group).
                   Use "PUBLISHED" to make updates public immediately.
            
        Returns:
            Dict[str, Any]: Summary of the synchronization process
        """
        logging.info(f"Starting synchronization of organizational units with status '{status}'...")
        
        # Check for duplicate stateCalendarId values in ODS data
        self._check_for_duplicate_ids_in_ods_staatskalender_data(org_data)
        
        # Fetch current org data from Dataspot
        dataspot_units = self._fetch_current_org_units()

        # Check for duplicate stateCalendarId values in Dataspot
        self._check_for_duplicate_ids_in_dataspot(dataspot_units)

        # Update mappings before making changes
        self.update_mappings_before_upload()
        
        # Check if this is an initial run (no org units in Dataspot)
        is_initial_run = len(dataspot_units) == 0
        
        # If this is an initial run, perform bulk upload
        if is_initial_run:
            logging.info(f"No organizational units found in Dataspot. Performing initial bulk upload with status '{status}'...")
            result = self._initialize_org_hierarchy_from_ods(org_data, status=status)
            
            # Update mappings
            source_ids = [str(org_unit['id']) for org_unit in org_data.get('results', [])]
            self.update_mappings_after_upload(source_ids)
            
            return {
                "status": result.get("status", "unknown"),
                "message": "Performed initial bulk upload as no existing organizational units were found",
                "details": result
            }
        
        # For incremental sync, transform both source and current data for comparison
        # Transform source org data to layers
        source_units_by_layer = OrgStructureTransformer.transform_to_layered_structure(org_data)
        
        # Index Dataspot units by Staatskalender ID
        dataspot_units_by_id = {
            str(unit.get(self.asset_id_field)): unit 
            for unit in dataspot_units
        }
        
        # Compare structures and identify changes
        changes = OrgStructureComparer.compare_structures(
            source_units_by_layer, dataspot_units_by_id)
        
        # If there are no changes, return early
        if not changes:
            return {
                "status": "no_changes",
                "message": "No changes detected between source and current organizational structures",
                "counts": {"total": 0, "created": 0, "updated": 0, "deleted": 0}
            }
        
        # Apply changes with the specified status
        stats = self.updater.apply_changes(changes, is_initial_run=False, status=status)
        
        # Update mappings after changes
        if changes:
            # Update all mappings to ensure hierarchy changes are captured properly
            self.update_mappings_after_upload()
        
        # Generate detailed report with all changes
        detailed_report = OrgStructureComparer.generate_detailed_sync_report(changes, stats=stats)
        
        return detailed_report
    
    def _fetch_current_org_units(self) -> List[Dict[str, Any]]:
        """
        Fetch current organizational units from Dataspot.
        
        Returns:
            List of organizational units currently in Dataspot
        """
        logging.info("Fetching current organizational units from Dataspot...")
        return self.client.get_all_assets_from_scheme(self.asset_type_filter)
    
    def update_mappings_after_upload(self, staatskalender_ids: Optional[List[str]] = None) -> int:
        """
        Update local mappings after uploading organizational units.
        
        Args:
            staatskalender_ids: List of Staatskalender IDs to update mappings for
            
        Returns:
            int: Number of mappings successfully updated
        """
        # Use the base implementation to update mappings
        return self._download_and_update_mappings(staatskalender_ids)
    
    def update_mappings_before_upload(self) -> int:
        """
        Update local mappings before uploading organizational units.
        
        Returns:
            int: Number of mappings successfully updated
        """
        # Update all mappings without filtering by ID
        return self._download_and_update_mappings()
