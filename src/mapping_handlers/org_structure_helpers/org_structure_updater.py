import logging
from typing import Dict, Any, List

from src.clients.base_client import BaseDataspotClient
from src.clients.helpers import url_join, escape_special_chars
from src.mapping_handlers.org_structure_helpers.org_structure_comparer import OrgUnitChange


def unescape_path_components(path: str) -> List[str]:
    """
    Unescape a path with special characters, doing the opposite of escape_special_chars.
    
    Takes a path where components may contain quoted parts and converts it into 
    a list of properly unescaped components.
    
    Args:
        path: The path with potentially quoted components
        
    Returns:
        List of unescaped path components
    """
    # Split by slashes but respect quoted parts
    components = []
    current_part = ""
    in_quotes = False
    i = 0
    
    while i < len(path):
        char = path[i]
        
        if char == '"':
            # Toggle quote state
            in_quotes = not in_quotes
            
            # Skip this character in output but include it in parsing
            i += 1
            
            # Handle doubled quotes inside quotes (escaped quotes)
            if in_quotes is False and i < len(path) and path[i] == '"':
                current_part += '"'  # Add a single quote
                in_quotes = True  # Still in quotes
                i += 1  # Skip the second quote
                
        elif char == '/' and not in_quotes:
            # End of a path component
            components.append(current_part)
            current_part = ""
            i += 1
        else:
            # Normal character
            current_part += char
            i += 1
    
    # Add the last part if there is one
    if current_part:
        components.append(current_part)
    
    return components


class OrgStructureUpdater:
    """
    Handles applying changes to organizational units in Dataspot.
    Responsible for creations, updates, and deletions of org units.
    """

    def __init__(self, client: BaseDataspotClient):
        """
        Initialize the OrgStructureUpdater.
        
        Args:
            client: BaseDataspotClient instance to use for API operations
        """
        self.client = client
        self.database_name = client.database_name
        self._org_units_cache = None
    
    def _fetch_all_org_units(self) -> Dict[str, Dict[str, Any]]:
        """
        Fetch all organizational units from Dataspot.
        
        Returns:
            Dict mapping org unit label to its full data, including UUID
        """
        if self._org_units_cache is not None:
            return self._org_units_cache
            
        # Define the filter function for org units
        org_filter = lambda asset: (
            asset.get('_type') == 'Collection' and 
            asset.get('stereotype') == 'organizationalUnit' and
            asset.get('stateCalendarId') is not None
        )
        
        # Fetch all org units
        logging.info("Pre-fetching all organizational units from Dataspot...")
        org_units = self.client.get_all_assets_from_scheme(org_filter)
        
        # Build lookup dictionaries by UUID and by label
        self._org_units_cache = {
            'by_uuid': {unit.get('id'): unit for unit in org_units if 'id' in unit},
            'by_label': {unit.get('label'): unit for unit in org_units if 'label' in unit},
            'by_sk_id': {str(unit.get('stateCalendarId')): unit
                        for unit in org_units 
                        if 'stateCalendarId' in unit and unit.get('stateCalendarId') is not None}
        }
        
        logging.info(f"Cached {len(org_units)} organizational units for quick lookup")
        return self._org_units_cache
    
    def apply_changes(self, changes: List[OrgUnitChange], is_initial_run: bool = False, status: str = "WORKING") -> Dict[str, int]:
        """
        Apply the identified changes to the system.
        
        Args:
            changes: List of changes to apply
            is_initial_run: Whether this is an initial run with no existing org units
            status: Status to set on updated org units. Defaults to "WORKING" (DRAFT group).
                   Use "PUBLISHED" to make updates public immediately.
            
        Returns:
            Dict[str, int]: Statistics about applied changes
        """
        if not changes:
            logging.info("No changes to apply")
            return {"created": 0, "updated": 0, "deleted": 0, "errors": 0, "directly_deleted": 0, "marked_for_deletion": 0}
            
        logging.info(f"Applying {len(changes)} changes...")
        
        # Pre-fetch all org units for efficient lookups
        self._fetch_all_org_units()
        
        stats = {
            "created": 0,
            "updated": 0,
            "deleted": 0,
            "errors": 0,
            "directly_deleted": 0,
            "marked_for_deletion": 0
        }
        
        # Group changes by type for clearer processing
        changes_by_type = {
            "create": [c for c in changes if c.change_type == "create"],
            "update": [c for c in changes if c.change_type == "update"],
            "delete": [c for c in changes if c.change_type == "delete"]
        }
        
        # Process deletions first
        self._process_deletions(list(reversed(changes_by_type["delete"])), stats)
        
        # Process creations before updates to ensure parent organizations exist
        # This fixes the issue where an update refers to a parent that needs to be created first
        self._process_creations(changes_by_type["create"], stats, status)
        
        # Refresh org units cache after creations to ensure new units are available for updates
        if changes_by_type["create"]:
            logging.info("Refreshing organization units cache after creating new units...")
            self._org_units_cache = None
            self._fetch_all_org_units()
        
        # Finally handle updates
        self._process_updates(changes_by_type["update"], is_initial_run, stats, status)
        
        logging.info(f"Change application complete: {stats['created']} created, {stats['updated']} updated, "
                     f"{stats['deleted']} deleted ({stats['directly_deleted']} empty collections directly deleted, "
                     f"{stats['marked_for_deletion']} non-empty collections marked for deletion), {stats['errors']} errors")
        
        return stats
    
    def _process_deletions(self, deletion_changes: List[OrgUnitChange], stats: Dict[str, int]) -> None:
        """
        Process deletion changes.
        
        Args:
            deletion_changes: List of deletion changes
            stats: Statistics dictionary to update
        """
        for change in deletion_changes:
            uuid = change.details.get("uuid")
            if not uuid:
                logging.warning(f"Cannot delete org unit '{change.title}' (ID: {change.staatskalender_id}) - missing UUID")
                stats["errors"] += 1
                continue
            
            # Construct endpoint for deletion
            endpoint = url_join('rest', self.database_name, 'collections', uuid, leading_slash=True)
            
            # First check if the asset still exists
            try:
                # Get the full asset data to check if it's empty
                asset_data = self.client._get_asset(endpoint)
                
                if asset_data is None:
                    # Asset doesn't exist anymore, just log and count it
                    logging.info(f"Org unit '{change.title}' (ID: {change.staatskalender_id}) already deleted in Dataspot, updating local mapping only")
                    stats["deleted"] += 1
                    continue
                
                # Check if the collection is empty based on _links field
                # Collection is empty if _links has exactly 2 entries: "self" and either "inCollection" or "inScheme"
                is_empty = False
                if "_links" in asset_data:
                    links = asset_data["_links"]
                    if len(links) == 2 and "self" in links and ("inCollection" in links or "inScheme" in links):
                        is_empty = True
                
                # Store whether the collection is empty in the change object for reporting
                change.details["is_empty"] = is_empty
                
                if is_empty:
                    # Collection is empty - directly delete it
                    logging.info(f"Directly deleting empty org unit '{change.title}' (ID: {change.staatskalender_id})")
                    try:
                        # Delete the collection
                        self.client._delete_asset(endpoint, force_delete=True)
                        stats["deleted"] += 1
                        stats["directly_deleted"] += 1
                    except Exception as e:
                        logging.error(f"Error deleting empty org unit '{change.title}' (ID: {change.staatskalender_id}): {str(e)}")
                        stats["errors"] += 1
                else:
                    # Collection is not empty - mark it for deletion review
                    logging.info(f"Marking non-empty org unit '{change.title}' (ID: {change.staatskalender_id}) for review")
                    try:
                        self.client._mark_asset_for_deletion(endpoint)
                        stats["deleted"] += 1
                        stats["marked_for_deletion"] += 1
                    except Exception as e:
                        logging.error(f"Error marking org unit '{change.title}' (ID: {change.staatskalender_id}) for review: {str(e)}")
                        stats["errors"] += 1
            except Exception as e:
                logging.error(f"Error processing deletion for org unit '{change.title}' (ID: {change.staatskalender_id}): {str(e)}")
                stats["errors"] += 1
    
    def _process_updates(self, update_changes: List[OrgUnitChange], is_initial_run: bool, stats: Dict[str, int], status: str) -> None:
        """
        Process update changes.
        
        Args:
            update_changes: List of update changes
            is_initial_run: Whether this is an initial run with no existing org units
            stats: Statistics dictionary to update
            status: Status to set on updated org units
        """
        # First, process label/name changes to ensure parent references are correct
        label_changes = [c for c in update_changes if "label" in c.details.get("changes", {})]
        other_changes = [c for c in update_changes if c not in label_changes]
        
        # Process label changes first (important for correct parent references)
        if label_changes:
            logging.info(f"Processing {len(label_changes)} label/name changes first")
            self._process_specific_changes(label_changes, stats, status)
            
        # Then process collection moves and other changes
        if other_changes:
            logging.info(f"Processing {len(other_changes)} other changes")
            self._process_specific_changes(other_changes, stats, status)
    
    def _process_specific_changes(self, changes: List[OrgUnitChange], stats: Dict[str, int], status: str) -> None:
        """
        Process specific change updates.
        
        Args:
            changes: List of changes to process
            stats: Statistics dictionary to update
            status: Status to set on updated org units
        """
        # Sort changes based on the source hierarchy layer (golden source)
        # Process root/parent collections first
        sorted_changes = sorted(changes, 
                               key=lambda c: len(unescape_path_components(c.details.get("source_unit", {}).get("inCollection", ""))))
        
        # Process each change
        for change in sorted_changes:
            uuid = change.details.get("uuid")
            if not uuid:
                logging.warning(f"Cannot update org unit '{change.title}' (ID: {change.staatskalender_id}) - missing UUID")
                stats["errors"] += 1
                continue
            
            # Get fresh asset data to ensure we have current state (especially for moves)
            try:
                endpoint = url_join('rest', self.database_name, 'assets', uuid, leading_slash=True)
                current_asset = self.client._get_asset(endpoint)
                if not current_asset:
                    logging.warning(f"Failed to get current state of asset {change.title} (ID: {uuid})")
                    continue
                
                # Update the change object with fresh data
                change.details["current_unit"] = current_asset
            except Exception as e:
                logging.error(f"Error fetching current asset state for '{change.title}' (ID: {uuid}): {str(e)}")
                stats["errors"] += 1
                continue
            
            # Construct endpoint for update
            endpoint = url_join('rest', self.database_name, 'collections', uuid, leading_slash=True)
            logging.info(f"Updating org unit '{change.title}' (ID: {change.staatskalender_id}) with status '{status}'")
            
            # Create update data with only necessary fields
            update_data = self._create_update_data(change)
            
            # If nothing changed (only _type and stereotype is in update_data), skip the update
            if len(update_data) <= 2:  # Just _type and stereotype
                logging.debug(f"No actual changes for org unit '{change.title}' after filtering, skipping update")
                continue
            
            try:
                # Update the asset with the specified status
                self.client._update_asset(endpoint, update_data, replace=False, status=status)
                stats["updated"] += 1
            except Exception as e:
                # Check if this is a 401 Unauthorized error
                if "401" in str(e) and "Unauthorized" in str(e):
                    logging.warning(f"Received 401 Unauthorized error updating org unit '{change.title}' (ID: {change.staatskalender_id}). Waiting 60 seconds and retrying...")
                    import time
                    time.sleep(60)
                    try:
                        # Try once more after waiting
                        self.client._update_asset(endpoint, update_data, replace=False, status=status)
                        logging.info(f"Successfully updated org unit '{change.title}' after retry")
                        stats["updated"] += 1
                    except Exception as retry_error:
                        # If retry also fails, log error and continue
                        logging.error(f"Error updating org unit '{change.title}' (ID: {change.staatskalender_id}) after retry: {str(retry_error)}")
                        stats["errors"] += 1
                else:
                    # For other errors, just log and continue
                    logging.error(f"Error updating org unit '{change.title}' (ID: {change.staatskalender_id}): {str(e)}")
                    stats["errors"] += 1
    
    def _create_update_data(self, change: OrgUnitChange) -> Dict[str, Any]:
        """
        Create update data with only the necessary fields to change.
        
        Args:
            change: The change to create update data for
            
        Returns:
            Dict[str, Any]: The update data
        """
        # Base required fields
        update_data = {
            "_type": "Collection",
            "stereotype": "organizationalUnit"
        }
        
        # Apply changes
        for field, change_info in change.details.get("changes", {}).items():
            if field == "customProperties":
                # For customProperties, only include what's changed
                if "customProperties" not in update_data:
                    update_data["customProperties"] = {}
                
                for prop, prop_change in change_info.items():
                    update_data["customProperties"][prop] = prop_change["new"]
            elif field == "inCollection":
                # For inCollection, handle parent changes with care
                # Extract the parent path from the inCollection value
                parent_path = change_info["new"]
                
                # Special handling for root collections
                if not parent_path:
                    # We need to move this collection to the scheme root level
                    logging.info(f"Moving collection '{change.title}' to scheme root level")
                    
                    # Remove from current collection and set back to scheme
                    update_data["inCollection"] = None
                    
                    # Get the scheme UUID
                    scheme_endpoint = url_join('rest', self.database_name, 'schemes', self.client.scheme_name, leading_slash=True)

                    scheme_data = self.client._get_asset(scheme_endpoint)
                    if scheme_data and "id" in scheme_data:
                        scheme_uuid = scheme_data["id"]
                        update_data["inScheme"] = scheme_uuid
                        logging.info(f"Setting inScheme to scheme UUID: {scheme_uuid}")
                    else:
                        error_msg = f"Could not retrieve scheme UUID for '{self.client.scheme_name}'"
                        logging.error(error_msg)
                        raise ValueError(error_msg)
                else:
                    # Improved approach: Find parent by its path components via our cached units
                    # We'll convert the path to components and find each unit by label
                    
                    # First, get all the path components
                    # Extract components correctly for all paths
                    path_components = unescape_path_components(parent_path)
                    
                    # The last component is the immediate parent we need to find
                    parent_label = path_components[-1] if path_components else ""
                    
                    # Look up the parent collection by its label in our cache
                    org_units = self._fetch_all_org_units()
                    parent_found = False
                    
                    # Find the parent Staatskalender ID from the source_unit
                    # This is available in the source data from ODS and avoids lookups by label
                    parent_sk_id = None
                    source_unit = change.details.get("source_unit", {})
                    if "stateCalendarParentId" in source_unit.get("customProperties"):
                        parent_sk_id = str(source_unit["customProperties"]["stateCalendarParentId"])
                    
                    # First try to find parent by Staatskalender ID which is more reliable
                    if parent_sk_id and parent_sk_id in org_units['by_sk_id']:
                        parent_unit = org_units['by_sk_id'][parent_sk_id]
                        parent_uuid = parent_unit.get('id')
                        if parent_uuid:
                            update_data["inCollection"] = parent_uuid
                            update_data["inScheme"] = None
                            logging.info(f"Found parent UUID: {parent_uuid} using Staatskalender ID: {parent_sk_id}")
                            parent_found = True
                    
                    # If parent not found, raise an error
                    if not parent_found:
                        error_msg = f"Failed to find parent collection. Parent SK ID: {parent_sk_id}, Parent label: '{parent_label}', Path: {parent_path}"
                        logging.error(error_msg)
                        raise ValueError(error_msg)
            else:
                # For simple fields, use the new value
                update_data[field] = change_info["new"]
        
        # If we have an empty customProperties after filtering, remove it
        if "customProperties" in update_data and not update_data["customProperties"]:
            del update_data["customProperties"]
        
        # Critical fix: Always include stateCalendarId in customProperties for PATCH requests
        # This ensures correct placement for the update operation
        if "stateCalendarId" not in update_data.get("customProperties", {}):
            if "customProperties" not in update_data:
                update_data["customProperties"] = {}
            update_data["customProperties"]["stateCalendarId"] = change.staatskalender_id
        
        # TODO (Renato): Clean this up; I think this is too complicated!
        #  Note: the inCollection field should always be a uuid when it is present.
        #  So, we will always come into the second of the 3 ifs.
        # Log the update data we're creating
        if "inCollection" in update_data:
            # Access the current unit name for move operations, as title may already contain the updated name
            current_name = change.details.get("current_unit", {}).get("label", change.title)
            
            if update_data["inCollection"] is None:
                if "inScheme" in update_data:
                    # Moving to root level
                    logging.info(f"Collection '{current_name}' will be moved to scheme root level")
                else:
                    logging.info(f"Collection '{current_name}' will have inCollection removed")
            elif isinstance(update_data["inCollection"], str) and not update_data["inCollection"].startswith("/"):
                logging.info(f"Collection '{current_name}' will be moved using inCollection UUID: {update_data['inCollection']}")
            else:
                logging.info(f"Collection '{current_name}' will be moved to path: {update_data['inCollection']}")
        elif "label" in update_data:
            # For renames, get the old name from the changes object
            old_name = change.details.get("changes", {}).get("label", {}).get("old", change.title)
            logging.info(f"Collection '{old_name}' will be renamed to: {update_data['label']}")
        
        return update_data
    
    def _process_creations(self, creation_changes: List[OrgUnitChange], stats: Dict[str, int], status: str = "WORKING") -> None:
        """
        Process creation changes.
        
        Args:
            creation_changes: List of creation changes
            stats: Statistics dictionary to update
            status: Status to set on created org units. Defaults to "WORKING" (DRAFT group).
        """
        if not creation_changes:
            return
            
        # Group create changes by their inCollection value (parent path)
        create_by_parent = {}
        for change in creation_changes:
            source_unit = change.details.get("source_unit", {})
            parent_path = source_unit.get("inCollection", "")
            
            if parent_path not in create_by_parent:
                create_by_parent[parent_path] = []
            
            # Add this unit to its parent group
            create_by_parent[parent_path].append(source_unit)
        
        # Process each parent group
        for parent_path, units in create_by_parent.items():
            logging.info(f"Creating {len(units)} org units under parent path '{parent_path}' with status '{status}'")
            
            try:
                # Bulk upload these units with the specified status
                response = self.client.bulk_create_or_update_assets(
                    scheme_name=self.client.scheme_name,
                    data=units,
                    operation="ADD",
                    dry_run=False,
                    status=status
                )
                
                # Check for errors
                errors = [message for message in response if isinstance(message, dict) and message.get('level') == 'ERROR']
                if errors:
                    logging.warning(f"Bulk creation completed with {len(errors)} errors")
                    stats["errors"] += len(errors)
                    stats["created"] += len(units) - len(errors)
                    for error in errors[:5]:  # Log first 5 errors
                        logging.error(f"  - {error.get('message', 'Unknown error')}")
                else:
                    stats["created"] += len(units)
                    logging.info(f"Successfully created {len(units)} units")
            except Exception as e:
                logging.error(f"Error during bulk creation of units under '{parent_path}': {str(e)}")
                stats["errors"] += len(units)
