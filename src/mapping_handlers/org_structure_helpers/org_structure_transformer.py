import logging
from typing import Dict, Any, List, Set, Tuple

from src.clients.helpers import escape_special_chars


class OrgStructureTransformer:
    """
    Handles transformation of organizational structure data between different formats.
    Extracts and transforms organization data from ODS API format to Dataspot format.
    """

    @staticmethod
    def transform_to_layered_structure(org_data: Dict[str, Any]) -> Dict[int, List[Dict[str, Any]]]:
        """
        Transform organization data into a layer-based structure.
        
        Args:
            org_data: Organization data from ODS API
            
        Returns:
            Dict[int, List[Dict[str, Any]]]: Dictionary with depth as key and list of org units at that depth
        """
        logging.info("Transforming organization data to layer structure...")
        
        # Build lookup and identify invalid organizations
        org_lookup, invalid_orgs = OrgStructureTransformer.build_organization_lookup(org_data)
        
        # Find root nodes
        root_nodes = OrgStructureTransformer.find_root_nodes(org_lookup, invalid_orgs)
        
        # Dictionary to track organization titles by ID for lookup
        org_title_by_id = {org_id: org.get('title', '').strip() for org_id, org in org_lookup.items()}
        
        # Dictionary to track the path components for each organization
        path_components_by_id = {}
        for root_id in root_nodes:
            path_components_by_id[root_id] = [org_title_by_id.get(root_id, '')]
        
        # Build a parent-child map for organization traversal
        parent_child_map = {}
        for org_id, org in org_lookup.items():
            parent_id = str(org.get('parent_id', '')).strip()
            if parent_id and parent_id.lower() != 'nan' and parent_id in org_lookup:
                if parent_id not in parent_child_map:
                    parent_child_map[parent_id] = []
                parent_child_map[parent_id].append(org_id)
        
        # Now construct hierarchical data with units organized by depth
        units_by_depth = {}
        processed_ids = set()
        
        # BFS traversal to build hierarchy level by level
        for depth in range(100):  # Safety limit to prevent infinite loops
            logging.info(f"Processing depth level {depth}")
            current_level_ids = []
            
            # For depth 0, start with root nodes
            if depth == 0:
                current_level_ids = root_nodes
            else:
                # Find all children of the previous level's nodes
                for parent_id in processed_ids:
                    if parent_id in parent_child_map:
                        for child_id in parent_child_map[parent_id]:
                            if child_id not in processed_ids and child_id not in invalid_orgs:
                                current_level_ids.append(child_id)
            
            # If no more nodes at this level, we're done
            if not current_level_ids:
                break
            
            # Initialize list for this depth if not already
            if depth not in units_by_depth:
                units_by_depth[depth] = []
            
            # Process each node at this level
            for org_id in current_level_ids:
                # Skip if already processed or invalid
                if org_id in processed_ids or org_id in invalid_orgs:
                    continue
                
                # Mark as processed
                processed_ids.add(org_id)
                
                # Get organization data
                if org_id not in org_lookup:
                    logging.warning(f"Organization ID {org_id} referenced but not found in data")
                    continue
                
                org = org_lookup[org_id]
                title = org.get('title', '')
                if not title:
                    logging.warning(f"Organization {org_id} missing title, skipping")
                    continue
                
                # Get URL
                url_website = org.get('url_website', '')
                
                # Create unit data
                unit_data = {
                    "_type": "Collection",
                    "label": title.strip(),
                    "stereotype": "organizationalUnit",
                    "stateCalendarId": org_id
                }
                
                # Add custom properties
                custom_properties = {}
                if url_website:
                    custom_properties["stateCalendarLink"] = url_website
                if org_id:
                    custom_properties["stateCalendarId"] = org_id
                
                # Set parent relationship using hierarchical business key
                parent_id = str(org.get('parent_id', '')).strip()
                if parent_id and parent_id.lower() != 'nan' and parent_id in org_lookup:
                    # Add parent's Staatskalender ID to custom properties
                    custom_properties["stateCalendarParentId"] = parent_id
                    
                    # Get the parent's path components
                    parent_path_components = OrgStructureTransformer.build_path_components(
                        parent_id, org_lookup, path_components_by_id)
                    
                    if parent_path_components:
                        # Escape each path component individually
                        escaped_components = []
                        
                        for comp in parent_path_components:
                            # Escape special characters in this component
                            escaped_comp = escape_special_chars(comp)
                            escaped_components.append(escaped_comp)
                        
                        # Join the escaped components with slashes
                        escaped_parent_path = '/'.join(escaped_components)
                        
                        unit_data["inCollection"] = escaped_parent_path
                
                # Set the custom properties on the unit_data
                if custom_properties:
                    unit_data["customProperties"] = custom_properties
                
                # Add to the units list for this depth
                units_by_depth[depth].append(unit_data)
        
        # Check for any organizations not included in the hierarchy
        not_processed = set(org_lookup.keys()) - processed_ids - invalid_orgs
        if not_processed:
            logging.warning(f"{len(not_processed)} organizations not included in the hierarchy due to circular references or other issues")
            
        logging.info(f"Organized units into {len(units_by_depth)} depth layers with {sum(len(units) for units in units_by_depth.values())} total units")
        return units_by_depth

    @staticmethod
    def build_organization_lookup(org_data: Dict[str, Any]) -> Tuple[Dict[str, Dict[str, Any]], Set[str]]:
        """
        Build a lookup dictionary for organizations and identify invalid entries.
        
        Args:
            org_data: Organization data from ODS API
            
        Returns:
            Tuple containing:
            - Dictionary mapping organization IDs to organization data
            - Set of invalid organization IDs to exclude
        """
        if not org_data or 'results' not in org_data:
            raise ValueError("Invalid organization data format. Data must contain a 'results' key.")
        
        # Build a lookup dictionary for quick access to organization by ID
        org_lookup = {str(org['id']): org for org in org_data['results']}
        logging.info(f"Processing {len(org_lookup)} organizations from input data")
        
        # Create a dictionary to track parent-child relationships
        parent_child_map = {}
        
        # Track organizations with missing parents - these will be excluded
        invalid_orgs = set()
        
        # Process each organization to identify invalid entries and build parent-child relationships
        for org_id, org in org_lookup.items():
            # Get parent ID
            parent_id = str(org.get('parent_id', '')).strip()
            if parent_id and parent_id.lower() != 'nan':
                # Check if parent exists
                if parent_id not in org_lookup:
                    logging.warning(f"Organization {org_id} ('{org.get('title', 'Unknown')}') references missing parent {parent_id}. Marking as invalid.")
                    # Mark this organization as invalid (has missing parent)
                    invalid_orgs.add(org_id)
                    continue
                
                # Add this org as a child of its parent
                if parent_id not in parent_child_map:
                    parent_child_map[parent_id] = []
                parent_child_map[parent_id].append(org_id)
            
            # Check children IDs for consistency
            children_ids = org.get('children_id', '')
            if children_ids:
                # Parse the children IDs - they might be in various formats
                try:
                    if isinstance(children_ids, str):
                        # Try to split by comma if it's a string
                        children_list = [id.strip() for id in children_ids.split(',')]
                    elif isinstance(children_ids, list):
                        children_list = [str(id).strip() for id in children_ids]
                    else:
                        children_list = [str(children_ids).strip()]
                    
                    # Add these children to the map if not already added by parent_id
                    if org_id not in parent_child_map:
                        parent_child_map[org_id] = []
                    
                    # Check each child
                    for child_id in children_list:
                        if child_id and child_id.lower() != 'nan':
                            if child_id not in org_lookup:
                                logging.warning(f"Organization {org_id} ('{org.get('title', 'Unknown')}') references missing child {child_id}")
                            else:
                                # Only add if not already in the list
                                if child_id not in parent_child_map[org_id]:
                                    parent_child_map[org_id].append(child_id)
                except Exception as e:
                    logging.error(f"Error processing children_id for organization {org_id}: {children_ids}")
                    logging.error(f"Exception: {str(e)}")
                    raise
        
        # Recursively mark descendants of invalid organizations as invalid too
        def mark_descendants_as_invalid(org_id: str) -> None:
            if org_id in parent_child_map:
                for child_id in parent_child_map[org_id]:
                    invalid_orgs.add(child_id)
                    mark_descendants_as_invalid(child_id)
        
        # Mark all descendants of invalid organizations as invalid too
        for org_id in list(invalid_orgs):
            mark_descendants_as_invalid(org_id)
            
        if invalid_orgs:
            logging.warning(f"Excluding {len(invalid_orgs)} organizations with missing parents or ancestors")
            
        return org_lookup, invalid_orgs

    @staticmethod
    def find_root_nodes(org_lookup: Dict[str, Dict[str, Any]], invalid_orgs: Set[str]) -> List[str]:
        """
        Find root nodes in the organization hierarchy.
        
        Args:
            org_lookup: Dictionary mapping organization IDs to organization data
            invalid_orgs: Set of invalid organization IDs to exclude
            
        Returns:
            List of root node IDs
        """
        root_nodes = []
        for org_id, org in org_lookup.items():
            # Skip organizations with missing parents
            if org_id in invalid_orgs:
                continue
                
            parent_id = str(org.get('parent_id', '')).strip()
            if not parent_id or parent_id.lower() == 'nan':
                logging.info(f"Organization {org_id} ('{org.get('title', 'Unknown')}') has no parent. Treating as root node.")
                root_nodes.append(org_id)
                
        if not root_nodes:
            raise ValueError("No root nodes found in organization data. Hierarchy cannot be built.")
        
        logging.info(f"Found {len(root_nodes)} root nodes")
        return root_nodes

    @staticmethod
    def build_path_components(
        org_id: str, 
        org_lookup: Dict[str, Dict[str, Any]], 
        path_components_by_id: Dict[str, List[str]]
    ) -> List[str]:
        """
        Build path components for an organization ID recursively.
        
        Args:
            org_id: Organization ID to build path for
            org_lookup: Dictionary mapping organization IDs to organization data
            path_components_by_id: Dictionary tracking path components by ID
            
        Returns:
            List of path components for this organization
        """
        # If path already calculated, return it
        if org_id in path_components_by_id:
            return path_components_by_id[org_id]
        
        # Get the organization's title
        title = org_lookup.get(org_id, {}).get('title', '')
        if not title:
            logging.warning(f"Organization {org_id} has no title, using ID as title")
            title = f"Unknown-{org_id}"
        
        # Get the parent ID
        parent_id = str(org_lookup.get(org_id, {}).get('parent_id', '')).strip()
        if not parent_id or parent_id.lower() == 'nan' or parent_id not in org_lookup:
            # If no valid parent, this is effectively a root node
            path_components = [title]
            path_components_by_id[org_id] = path_components
            return path_components
        
        # Get the parent's path components recursively
        parent_path_components = OrgStructureTransformer.build_path_components(
            parent_id, org_lookup, path_components_by_id)
        
        # Create the full path components by appending this org's title
        path_components = parent_path_components + [title]
        
        # Store the components in the mapping
        path_components_by_id[org_id] = path_components
        return path_components
