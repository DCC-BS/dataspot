import logging
from typing import Dict, Any, List, Set, NamedTuple


class OrgUnitChange(NamedTuple):
    """Class to track changes to organizational units"""
    staatskalender_id: str
    title: str
    change_type: str  # "create", "update", "delete"
    details: Dict[str, Any]  # Details about the change


class OrgStructureComparer:
    """
    Compares organizational structures and identifies changes needed.
    Handles the comparison between source data (e.g., from ODS) and
    target data (e.g., from Dataspot).
    """
    
    @staticmethod
    def compare_structures(
        source_units_by_layer: Dict[int, List[Dict[str, Any]]], 
        dataspot_units_by_id: Dict[str, Dict[str, Any]]
    ) -> List[OrgUnitChange]:
        """
        Compare the source organizational structure with the current structure.
        
        Args:
            source_units_by_layer: Organization units from source (ODS) organized by depth layer
            dataspot_units_by_id: Organization units from current system indexed by ID
            
        Returns:
            List[OrgUnitChange]: List of changes to be applied
        """
        logging.info("Comparing source and current organizational structures...")
        
        changes = []
        
        # Track all source IDs to later identify deletions
        source_ids = set()
        
        # Process each layer in the source structure
        for layer, units in source_units_by_layer.items():
            logging.info(f"Comparing layer {layer} with {len(units)} units")
            
            for unit in units:
                staatskalender_id = str(unit.get("id_im_staatskalender", ""))
                if not staatskalender_id:
                    logging.warning("Found unit without Staatskalender ID, skipping")
                    continue
                
                # Add to set of source IDs for later deletion check
                source_ids.add(staatskalender_id)
                
                title = unit.get("label", "")
                
                # Check if this unit exists in Dataspot
                if staatskalender_id in dataspot_units_by_id:
                    # Unit exists, check for changes
                    dataspot_unit = dataspot_units_by_id[staatskalender_id]
                    changes_needed = OrgStructureComparer.check_for_unit_changes(unit, dataspot_unit)
                    
                    if changes_needed:
                        changes.append(OrgUnitChange(
                            staatskalender_id=staatskalender_id,
                            title=title,
                            change_type="update",
                            details={
                                "uuid": dataspot_unit.get("id"),
                                "changes": changes_needed,
                                "source_unit": unit,
                                "current_unit": dataspot_unit
                            }
                        ))
                else:
                    # Unit doesn't exist, mark for creation
                    changes.append(OrgUnitChange(
                        staatskalender_id=staatskalender_id,
                        title=title,
                        change_type="create",
                        details={
                            "source_unit": unit
                        }
                    ))
        
        # Check for units that need to be deleted (in Dataspot but not in source)
        for staatskalender_id, unit in dataspot_units_by_id.items():
            if staatskalender_id not in source_ids:
                title = unit.get("label", f"Unknown ({staatskalender_id})")
                changes.append(OrgUnitChange(
                    staatskalender_id=staatskalender_id,
                    title=title,
                    change_type="delete",
                    details={
                        "uuid": unit.get("id"),
                        "current_unit": unit
                    }
                ))
        
        logging.info(f"Identified {len(changes)} changes needed: "
                    f"{sum(1 for c in changes if c.change_type == 'create')} creations, "
                    f"{sum(1 for c in changes if c.change_type == 'update')} updates, "
                    f"{sum(1 for c in changes if c.change_type == 'delete')} deletions")
        
        return changes
    
    @staticmethod
    def check_for_unit_changes(source_unit: Dict[str, Any], dataspot_unit: Dict[str, Any]) -> Dict[str, Any]:
        """
        Check if there are differences between source and target versions of an org unit.
        
        Args:
            source_unit: Organization unit data from source
            dataspot_unit: Organization unit data from target
            
        Returns:
            Dict[str, Any]: Dictionary of changes needed (empty if no changes)
        """
        changes = {}
        
        # Check label (title)
        if source_unit.get("label") != dataspot_unit.get("label"):
            changes["label"] = {
                "old": dataspot_unit.get("label"),
                "new": source_unit.get("label")
            }
        
        # Check custom properties - specifically link_zum_staatskalender
        # Source units have properties in customProperties, but download API returns properties flat
        source_url = source_unit.get("customProperties", {}).get("link_zum_staatskalender", "")
        dataspot_url = dataspot_unit.get("link_zum_staatskalender", "")
        
        if source_url != dataspot_url:
            if "customProperties" not in changes:
                changes["customProperties"] = {}
            
            changes["customProperties"]["link_zum_staatskalender"] = {
                "old": dataspot_url,
                "new": source_url
            }
        
        # Check for parent_id_im_staatskalender in source and target
        source_parent_id = source_unit.get("customProperties", {}).get("parent_id_im_staatskalender", None)
        dataspot_parent_id = dataspot_unit.get("parent_id_im_staatskalender", None)
        
        # Convert both to string for comparison
        source_parent_id_str = str(source_parent_id) if source_parent_id is not None else None
        dataspot_parent_id_str = str(dataspot_parent_id) if dataspot_parent_id is not None else None
        
        # Check if parent ID is missing or different
        if source_parent_id_str != dataspot_parent_id_str:
            if "customProperties" not in changes:
                changes["customProperties"] = {}
            
            changes["customProperties"]["parent_id_im_staatskalender"] = {
                "old": dataspot_parent_id,
                "new": source_parent_id
            }
            logging.info(f"Detected parent ID change for '{dataspot_unit.get('label', '')} (ID: {dataspot_unit['id_im_staatskalender']})': '{dataspot_parent_id}' → '{source_parent_id}'")
        
        # Check inCollection to detect if a collection has been moved
        # Source has the correct path based on ODS data, Dataspot has current path
        source_path = source_unit.get("inCollection", "")
        dataspot_path = dataspot_unit.get("inCollection", "")
        
        if source_path != dataspot_path:
            logging.info(f"Detected path difference for '{dataspot_unit.get('label', '')}': '{dataspot_path}' → '{source_path}'")
            changes["inCollection"] = {
                "old": dataspot_path,
                "new": source_path
            }
        
        return changes

    # TODO (Renato): Delete this method? Was replaced by generate_detailed_sync_report
    @staticmethod
    def generate_sync_summary(changes: List[OrgUnitChange]) -> Dict[str, Any]:
        """
        Generate a human-readable summary of the synchronization.
        
        Args:
            changes: List of changes that were identified
            
        Returns:
            Dict[str, Any]: Summary dictionary with statistics and details
        """
        # Count changes by type
        counts = {
            "total": len(changes),
            "created": sum(1 for c in changes if c.change_type == "create"),
            "updated": sum(1 for c in changes if c.change_type == "update"),
            "deleted": sum(1 for c in changes if c.change_type == "delete")
        }
        
        # Generate summary text
        summary_text = f"Organizational structure synchronization completed with {counts['total']} changes: " \
                      f"{counts['created']} creations, {counts['updated']} updates, {counts['deleted']} deletions."
        
        # Prepare detailed statistics for each change type
        details = {}
        
        # Add sample creations
        if counts["created"] > 0:
            creations = [c for c in changes if c.change_type == "create"]
            details["creations"] = {
                "count": counts["created"],
                "samples": [f"'{c.title}' (ID: {c.staatskalender_id})" for c in creations[:5]]
            }
        
        # Add sample updates
        if counts["updated"] > 0:
            updates = [c for c in changes if c.change_type == "update"]
            details["updates"] = {
                "count": counts["updated"],
                "samples": [f"'{c.title}' (ID: {c.staatskalender_id})" for c in updates[:5]]
            }
        
        # Add sample deletions
        if counts["deleted"] > 0:
            deletions = [c for c in changes if c.change_type == "delete"]
            details["deletions"] = {
                "count": counts["deleted"],
                "samples": [f"'{c.title}' (ID: {c.staatskalender_id})" for c in deletions[:5]]
            }
        
        # Create the complete summary
        summary = {
            "status": "success",
            "message": summary_text,
            "counts": counts,
            "details": details
        }
        
        # Log some details
        logging.info(f"Organizational structure synchronization completed with {counts['total']} changes: "
                    f"{counts['created']} creations, {counts['updated']} updates, {counts['deleted']} deletions.")
        
        if "updates" in details:
            logging.info(f"Sample updates: {', '.join(details['updates']['samples'][:3])}")
        
        if "creations" in details:
            logging.info(f"Sample creations: {', '.join(details['creations']['samples'][:3])}")
        
        if "deletions" in details:
            logging.info(f"Sample deletions: {', '.join(details['deletions']['samples'][:3])}")
        
        return summary
    
    @staticmethod
    def generate_detailed_sync_report(changes: List[OrgUnitChange], stats: Dict[str, int] = None) -> Dict[str, Any]:
        """
        Generate a detailed report of all synchronization changes.
        Includes complete information about every change with old and new values.
        
        Args:
            changes: List of changes that were identified
            stats: Optional dictionary with additional statistics (like directly_deleted and marked_for_deletion)
            
        Returns:
            Dict[str, Any]: Detailed report dictionary with complete information
        """
        # Count changes by type
        counts = {
            "total": len(changes),
            "created": sum(1 for c in changes if c.change_type == "create"),
            "updated": sum(1 for c in changes if c.change_type == "update"),
            "deleted": sum(1 for c in changes if c.change_type == "delete")
        }
        
        # Add extra statistics if provided
        if stats:
            for key, value in stats.items():
                if key not in counts:  # Don't override existing counts
                    counts[key] = value
        
        # Ensure we always have values for these fields
        directly_deleted = counts.get('directly_deleted', 0)
        marked_for_deletion = counts.get('marked_for_deletion', 0)
        
        # Generate report text
        report_text = f"Organizational structure synchronization completed with {counts['total']} changes: " \
                      f"{counts['created']} creations, {counts['updated']} updates, {counts['deleted']} deletions " \
                      f"({directly_deleted} empty collections directly deleted, " \
                      f"{marked_for_deletion} non-empty collections marked for review)."
        
        # Prepare detailed information for each change type
        details = {}
        
        # Process creations - show all created units with their properties
        if counts["created"] > 0:
            creations = [c for c in changes if c.change_type == "create"]
            creation_details = []
            
            for c in creations:
                source_unit = c.details.get("source_unit", {})
                creation_detail = {
                    "title": c.title,
                    "staatskalender_id": c.staatskalender_id,
                    "properties": {
                        "label": source_unit.get("label", ""),
                        "inCollection": source_unit.get("inCollection", ""),
                        "link_zum_staatskalender": source_unit.get("customProperties", {}).get("link_zum_staatskalender", "")
                    }
                }
                
                # Add UUID if available (might be populated after creation)
                if "uuid" in c.details:
                    creation_detail["uuid"] = c.details["uuid"]
                
                creation_details.append(creation_detail)
            
            details["creations"] = {
                "count": counts["created"],
                "items": creation_details
            }
        
        # Process updates - show all updated units with old and new values for changed fields
        if counts["updated"] > 0:
            updates = [c for c in changes if c.change_type == "update"]
            update_details = []
            
            for c in updates:
                update_info = {
                    "title": c.title,
                    "staatskalender_id": c.staatskalender_id,
                    "uuid": c.details.get("uuid", ""),
                    "changed_fields": {}
                }
                
                # Extract all changed fields with old and new values
                changes_dict = c.details.get("changes", {})
                for field, change_info in changes_dict.items():
                    if field == "customProperties":
                        # Handle nested custom properties
                        for prop, prop_change in change_info.items():
                            update_info["changed_fields"][f"customProperties.{prop}"] = {
                                "old_value": prop_change.get("old", ""),
                                "new_value": prop_change.get("new", "")
                            }
                    else:
                        # Handle regular fields
                        update_info["changed_fields"][field] = {
                            "old_value": change_info.get("old", ""),
                            "new_value": change_info.get("new", "")
                        }
                
                update_details.append(update_info)
            
            details["updates"] = {
                "count": counts["updated"],
                "items": update_details
            }
        
        # Process deletions - show all deleted units
        if counts["deleted"] > 0:
            deletions = [c for c in changes if c.change_type == "delete"]
            deletion_details = []
            
            for c in deletions:
                current_unit = c.details.get("current_unit", {})
                deletion_details.append({
                    "title": c.title,
                    "staatskalender_id": c.staatskalender_id,
                    "uuid": c.details.get("uuid", ""),
                    "inCollection": current_unit.get("inCollection", ""),
                    "is_empty": c.details.get("is_empty", False)
                })
            
            details["deletions"] = {
                "count": counts["deleted"],
                "items": deletion_details
            }
        
        # Create the complete detailed report
        report = {
            "status": "success",
            "message": report_text,
            "counts": counts,
            "details": details
        }
        
        # Log that a detailed report was generated
        logging.info("Generated detailed synchronization report with complete change information")
        
        return report