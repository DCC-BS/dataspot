import logging
import time
from typing import List, Dict, Any

from src.clients.base_client import BaseDataspotClient
from src.clients.helpers import url_join, get_uuid_from_response
from src.mapping_handlers.base_dataspot_handler import BaseDataspotHandler
from src.mapping_handlers.base_dataspot_mapping import BaseDataspotMapping


class DatasetComponentMapping(BaseDataspotMapping):
    """
    A lookup table that maps ODS IDs to Dataspot asset type, UUID, and optionally inCollection.
    Stores the mapping in a CSV file for persistence. Handles only dataset components.
    The REST endpoint is constructed dynamically.
    """

    def __init__(self, database_name: str, scheme: str):
        """
        Initialize the mapping table for dataset components.
        The CSV filename is derived from the database_name and scheme.

        Args:
            database_name (str): Name of the database to use for file naming.
                                 Example: "feature-staatskalender_TDM_ods-components-dataspot-mapping.csv"
            scheme (str): Name of the scheme (e.g., 'TDM')
        """
        super().__init__(database_name, "ods_id", "ods-components-dataspot", scheme)


class DatasetComponentHandler(BaseDataspotHandler):
    """
    Handler for dataset component synchronization operations in Dataspot.
    Provides methods to sync dataset components between ODS and Dataspot.
    """
    # Set configuration values for the base handler
    asset_id_field = 'ODS_ID'
    
    def __init__(self, client: BaseDataspotClient):
        """
        Initialize the DatasetComponentHandler.
        
        Args:
            client: BaseDataspotClient instance to use for API operations
        """
        # Call parent's __init__ method first
        super().__init__(client)
        
        # Initialize the dataset component mapping
        self.mapping = DatasetComponentMapping(database_name=client.database_name, scheme=client.scheme_name_short)

        # Set the asset type filter based on asset_id_field
        self.asset_type_filter = lambda asset: (asset.get('_type') == 'UmlClass' and 
                                                asset.get('stereotype') == 'ogd_dataset' and 
                                                asset.get(self.asset_id_field) is not None)

        # Define the datatype mapping
        self._datatype_mapping = {
            'text': '/Datentypmodell/Text',
            'int': '/Datentypmodell/Ganzzahl',
            'boolean': '/Datentypmodell/Wahrheitswert',
            'double': '/Datentypmodell/Dezimalzahl',
            'datetime': '/Datentypmodell/Zeitpunkt',
            'date': '/Datentypmodell/Datum',
            'geo_point_2d': '/Datentypmodell/geo_point_2d',
            'geo_shape': '/Datentypmodell/geo_shape',
            'file': '/Datentypmodell/Binärdaten',
            'json_blob': '/Datentypmodell/Text'
        }

        # Initialize cache for datatype UUIDs
        self._datatype_uuid_cache = {}
        
        # Check for special characters in the default path and name
        if any('/' in folder for folder in self.client.ods_imports_collection_path) \
            or any('.' in folder for folder in self.client.ods_imports_collection_path) \
            or ('/' in self.client.ods_imports_collection_name) \
            or '.' in self.client.ods_imports_collection_name:
            logging.error("The default path or name in config.py contains special characters ('/' or '.') that need escaping. This functionality is not yet supported and needs to be properly implemented as needed.")
            raise ValueError("The default path or name in config.py contains special characters ('/' or '.') that need escaping. This functionality is not yet supported and needs to be properly implemented as needed.")

        if self.client.ods_imports_collection_path:
            self.default_component_path_full = url_join(*self.client.ods_imports_collection_path, self.client.ods_imports_collection_name)
        else:
            self.default_component_path_full = self.client.ods_imports_collection_name

        logging.debug(f"Default component path: {self.default_component_path_full}")
        
        # Prefetch all datatype UUIDs
        self._prefetch_datatype_uuids()
        
        # Update mappings during initialization to ensure fresh data
        self.update_mappings_before_upload()

    def _prefetch_datatype_uuids(self):
        """
        Prefetch all datatype UUIDs and store them in the cache to reduce API calls.
        """
        import config
        
        logging.info("Prefetching datatype UUIDs...")
        
        for ods_type, datatype_path in self._datatype_mapping.items():
            # Use the last part as the type name
            parts = datatype_path.strip('/').split('/')
            type_name = parts[-1]
            
            # Build path to datatype
            dtype_endpoint = f"/rest/{self.client.database_name}/schemes/{config.datatype_scheme_name}/datatypes/{type_name}"
            
            # Get datatype UUID
            response = self.client._get_asset(dtype_endpoint)
            if response:
                self._datatype_uuid_cache[ods_type.lower()] = response.get('id')
                logging.debug(f"Cached datatype UUID for {ods_type}: {self._datatype_uuid_cache[ods_type.lower()]}")
            else:
                logging.warning(f"Could not find datatype for {ods_type}")
        
        logging.info(f"Prefetched {len(self._datatype_uuid_cache)} datatype UUIDs")

    def sync_dataset_components(self, ods_id: str, name: str, columns: List[Dict[str, Any]]) -> Dict[str, Any]:
        """
        Create or update a TDM dataobject for a dataset with its columns as attributes.
        
        Args:
            ods_id (str): The ODS ID of the dataset
            name (str): The name of the dataset/dataobject
            columns (List[Dict[str, Any]]): List of column definitions, each containing:
                - label: Human-readable label
                - name: Technical column name
                - type: Data type of the column
                - description: Description of the column
                
        Returns:
            Dict[str, Any]: Result of the operation with status and details
        """
        # Define helper functions for cleaning and truncating descriptions
        def clean_description(desc):
            """Clean description by replacing newlines with spaces"""
            if not desc:
                return ""
            return str(desc).replace('\r\n', ' ').replace('\n', ' ').replace('\r', ' ')
        
        def clean_description_short(desc):
            """Create shortened version of the description"""
            if not desc:
                return ""
            cleaned = clean_description(desc)
            if len(cleaned) > 60:
                return cleaned[:60].rsplit(' ', 1)[0] + " ..."
            return cleaned
        
        ods_link = f"https://data.bs.ch/explore/dataset/{ods_id}/"

        # Prepare dataobject data
        dataobject = {
            "_type": "UmlClass",
            "label": name,
            "stereotype": "ogd_dataset",
            "customProperties": {
                "ODS_ID": ods_id,
                "ODS_LINK": ods_link
            }
        }
        
        # Track if this is a new object or an update
        is_new = True
        asset_uuid = None
        
        # Check if asset already exists with this ODS_ID using mapping
        existing_entry = self.mapping.get_entry(ods_id)
        
        if existing_entry:
            # Asset exists in mapping, verify it still exists in Dataspot
            _type, uuid, _inCollection = existing_entry
            asset_uuid = uuid
            
            # Get the endpoint for this asset
            endpoint = f"/rest/{self.client.database_name}/assets/{asset_uuid}"
            
            # Check if asset still exists
            current_asset = self.client._get_asset(endpoint=endpoint)
            if current_asset:
                logging.info(f"Found existing dataobject for dataset {ods_id} (UUID: {asset_uuid})")
                is_new = False
                
                # Update the existing dataobject
                logging.info(f"Updating existing dataobject properties for dataset {ods_id}...")
                response = self.client._update_asset(endpoint=endpoint, data=dataobject, replace=False, status="PUBLISHED")
            else:
                # Asset doesn't exist in Dataspot anymore, remove from mapping
                logging.warning(f"Dataobject for dataset {ods_id} found in mapping but not in Dataspot. Removing from mapping.")
                self.mapping.remove_entry(ods_id)
                asset_uuid = None
                is_new = True
        
        # If not found in mapping or not existing in Dataspot, check if asset exists with this ODS_ID
        if not existing_entry or not asset_uuid:
            asset_filter = lambda asset: (
                asset.get('_type') == 'UmlClass' and
                asset.get('stereotype') == 'ogd_dataset' and
                asset.get('ODS_ID') == ods_id
            )
            
            existing_assets = self.client.get_all_assets_from_scheme(filter_function=asset_filter)
    
            if existing_assets:
                if len(existing_assets) > 1:
                    logging.error(f"Found {len(existing_assets)} assets with ODS_ID {ods_id} in the {self.client.scheme_name_short} when only one should exist!")
                    raise ValueError(f"Multiple assets found with ODS_ID {ods_id}")
                else:
                    logging.info(f"Found existing dataobject for dataset {ods_id} (UUID: {existing_assets[0].get('id')})")
    
                # Asset exists, update it
                is_new = False
                asset = existing_assets[0]
                asset_uuid = asset.get('id')
                
                if not asset_uuid:
                    raise ValueError(f"Found existing dataobject for {ods_id} but could not get UUID")
                    
                # Update the existing dataobject
                endpoint = f"/rest/{self.client.database_name}/assets/{asset_uuid}"
                logging.info(f"Updating existing dataobject properties for dataset {ods_id}...")
                response = self.client._update_asset(endpoint=endpoint, data=dataobject, replace=False, status="PUBLISHED")
                
                # Add to mapping
                self.mapping.add_entry(ods_id, "UmlClass", asset_uuid, self.default_component_path_full)
        
        # If still no asset_uuid, create a new dataobject
        if not asset_uuid:
            # Create new dataobject in the ODS-Imports collection
            collection_uuid = self.client._ods_imports_collection.get('id')
            if not collection_uuid:
                raise ValueError("Failed to get collection UUID")
            
            # Create the dataobject
            logging.info(f"Creating new dataobject for dataset {ods_id} in collection {self.client.ods_imports_collection_name}...")
            endpoint = f"/rest/{self.client.database_name}/collections/{collection_uuid}/assets"
            response = self.client._create_asset(endpoint=endpoint, data=dataobject, status="PUBLISHED")
            asset_uuid = response.get('id')
            
            if not asset_uuid:
                raise ValueError(f"Failed to create dataobject for {ods_id}")
            else:
                logging.info(f"Successfully created new dataobject (UUID: {asset_uuid})")
                # Add to mapping
                self.mapping.add_entry(ods_id, "UmlClass", asset_uuid, self.default_component_path_full)
        
        # Process attributes (columns)
        attributes_endpoint = f"/rest/{self.client.database_name}/classifiers/{asset_uuid}/attributes"
        
        # Get existing attributes to determine what to update vs create
        existing_attributes = {}
        
        try:
            logging.info(f"Retrieving existing attributes for dataset {ods_id}...")
            attrs_response = self.client._get_asset(attributes_endpoint)
            if attrs_response and '_embedded' in attrs_response and 'attributes' in attrs_response['_embedded']:
                for attr in attrs_response['_embedded']['attributes']:
                    technical_name = attr.get('physicalName')
                    if not technical_name:
                        logging.error(f"Attribute {attr['label']} is missing a physicalName (technical name)! Skipping...")
                        raise ValueError(f"Attribute {attr['label']} is missing a physicalName (technical name)! Skipping...")
                    existing_attributes[attr['physicalName']] = attr
                logging.info(f"Found {len(existing_attributes)} existing attributes")
            else:
                logging.info(f"No existing attributes found for dataset {ods_id}")
        except Exception as e:
            # Log the error but continue with empty existing_attributes
            logging.warning(f"Failed to retrieve existing attributes for {ods_id}: {str(e)}")
        
        # Track changes
        created_attrs = []
        updated_attrs = []
        unchanged_attrs = []
        deleted_attrs = []
        
        # Store detailed field changes
        field_changes = {}
        
        # Process each column as an attribute
        logging.info(f"Processing {len(columns)} columns as attributes...")
        for column in columns:
            # Map ODS types to UML data types
            datatype_uuid = self._get_datatype_uuid(column['type'])
            
            attribute = {
                "_type": "UmlAttribute",
                "label": column['label'],
                "physicalName": column['name'],
                "hasRange": datatype_uuid
            }
            
            # Add description if available
            # Note: We write the description as title to make it globally visible in dataspot.
            if 'description' in column and column['description']:
                # Store original description with newlines in description field
                attribute['description'] = column['description']
                
                # Create clean and shortened version of the description for title
                attribute['title'] = clean_description_short(column['description'])
            
            # Check if attribute exists to determine if update or create
            if column['name'] in existing_attributes:
                # Get existing attribute data
                existing_attr = existing_attributes[column['name']]
                attr_uuid = existing_attr.get('id')
                
                # Check if anything changed
                if (existing_attr.get('label') == column['label'] and
                    existing_attr.get('hasRange') == datatype_uuid and
                    existing_attr.get('description') == column.get('description') and
                    existing_attr.get('title') == clean_description_short(column.get('description'))):
                    # Attribute is unchanged
                    unchanged_attrs.append(column['name'])
                    logging.debug(f"Attribute '{column['name']}' is unchanged")
                else:
                    # Track changes in detail with before/after values
                    attr_changes = {}
                    
                    if existing_attr.get('label') != column['label']:
                        attr_changes['label'] = {
                            'old_value': existing_attr.get('label'),
                            'new_value': column['label']
                        }
                    
                    if existing_attr.get('hasRange') != datatype_uuid:
                        attr_changes['datatype'] = {
                            'old_value': existing_attr.get('hasRange'),
                            'new_value': datatype_uuid
                        }
                    
                    # Calculate cleaned description for comparison
                    cleaned_short = clean_description_short(column.get('description'))
                    
                    # Check changes in title (cleaned/truncated description)
                    if existing_attr.get('title') != cleaned_short:
                        attr_changes['title'] = {
                            'old_value': existing_attr.get('title'),
                            'new_value': cleaned_short
                        }
                    
                    # Check changes in actual description (raw with newlines)
                    if existing_attr.get('description') != column.get('description'):
                        attr_changes['description'] = {
                            'old_value': existing_attr.get('description'),
                            'new_value': column.get('description')
                        }
                    
                    # Store changes for this attribute
                    if attr_changes:
                        field_changes[column['name']] = attr_changes
                    
                    # Log the changes
                    changes_desc = []
                    if 'label' in attr_changes:
                        changes_desc.append(f"label: '{attr_changes['label']['old_value']}' → '{attr_changes['label']['new_value']}'")
                    if 'datatype' in attr_changes:
                        changes_desc.append(f"datatype changed")
                    if 'title' in attr_changes:
                        changes_desc.append(f"title (truncated description) changed")
                    if 'description' in attr_changes:
                        changes_desc.append(f"full description changed")
                    
                    logging.info(f"Updating attribute '{column['name']}': {', '.join(changes_desc)}")
                    
                    # Update the attribute
                    attr_endpoint = f"/rest/{self.client.database_name}/attributes/{attr_uuid}"
                    self.client._update_asset(endpoint=attr_endpoint, data=attribute, replace=False, status="PUBLISHED")
                    updated_attrs.append(column['name'])
                    time.sleep(1)
                
                # Remove from existing_attributes to track what's left for deletion
                del existing_attributes[column['name']]
            else:
                # Create new attribute
                logging.info(f"Creating new attribute '{column['name']}' with type '{column['type']}'")
                self.client._create_asset(endpoint=attributes_endpoint, data=attribute, status="PUBLISHED")
                created_attrs.append(column['name'])
                time.sleep(1)
        
        # Handle deletions - any attributes still in existing_attributes need to be removed
        if existing_attributes:
            logging.info(f"Found {len(existing_attributes)} attributes to delete")
            
        for attr_name, attr_data in existing_attributes.items():
            attr_uuid = attr_data.get('id')
            if attr_uuid:
                attr_composed_by = attr_data.get('_links', {}).get('composedBy', {}).get('href')
                compositions_asset = self.client._get_asset(endpoint=attr_composed_by)
                compositions_list = compositions_asset.get('_embedded', {}).get('composedBy', {})
                for composition_asset in compositions_list:
                    composition_endpoint = composition_asset.get('_links', {}).get('self', {}).get('href')
                    if composition_endpoint:
                        logging.info(f"Deleting link from dataset composition to dataobject attribute '{attr_name}'")
                        # Note: Here, we use the TDMClient to delete an asset from the DNK, but I think this is fine.
                        self.client._delete_asset(endpoint=composition_endpoint)
                logging.info(f"Deleting unused attribute '{attr_name}'")
                attr_endpoint = f"/rest/{self.client.database_name}/attributes/{attr_uuid}"
                self.client._delete_asset(attr_endpoint)
                deleted_attrs.append(attr_name)
        
        # Add a delay if any attributes were created to avoid hitting API rate limits
        if created_attrs or updated_attrs:
            logging.info(f"Added {len(created_attrs)} new attributes and {len(updated_attrs)} updated attributes. Waiting 10 seconds to avoid API rate limits...")
            time.sleep(10)
        
        # Asset link for reference in results
        dataspot_link = f"{self.client.base_url}/web/{self.client.database_name}/assets/{asset_uuid}" if asset_uuid else ""
        
        # Prepare result
        result = {
            "status": "success",
            "message": f"{'Created new' if is_new else 'Updated existing'} dataobject for dataset {ods_id}",
            "uuid": asset_uuid,
            "link": dataspot_link,
            "ods_id": ods_id,
            "title": name,
            "counts": {
                "created_attributes": len(created_attrs),
                "updated_attributes": len(updated_attrs),
                "unchanged_attributes": len(unchanged_attrs),
                "deleted_attributes": len(deleted_attrs),
                "total_changes": len(created_attrs) + len(updated_attrs) + len(deleted_attrs)
            },
            "details": {
                "created_attributes": created_attrs,
                "updated_attributes": updated_attrs,
                "unchanged_attributes": unchanged_attrs,
                "deleted_attributes": deleted_attrs,
                "field_changes": field_changes
            },
            "is_new": is_new
        }
        
        # Log summary of changes
        if is_new:
            logging.info(f"Created new dataobject for dataset {ods_id} with {len(created_attrs)} attributes")
        else:
            changes = []
            if created_attrs:
                changes.append(f"{len(created_attrs)} attributes created")
            if updated_attrs:
                changes.append(f"{len(updated_attrs)} attributes updated")
            if deleted_attrs:
                changes.append(f"{len(deleted_attrs)} attributes deleted")
            if unchanged_attrs:
                changes.append(f"{len(unchanged_attrs)} attributes unchanged")
                
            if changes:
                logging.info(f"Updated dataobject for dataset {ods_id} with changes: {', '.join(changes)}")
            else:
                logging.info(f"No changes made to dataobject for dataset {ods_id}")
        
        # Update mappings after changes
        self.update_mappings_after_upload([ods_id])
        
        # Save mapping to CSV
        self.mapping.save_to_csv()
        
        return result
        
    def _get_datatype_uuid(self, ods_type: str) -> str:
        """
        Map ODS data type to Dataspot datatype UUID.
        
        Args:
            ods_type (str): The ODS data type
            
        Returns:
            str: UUID of the corresponding datatype in Dataspot
        """
        ods_type_lower = ods_type.lower()
        
        # Check if UUID is in cache
        if ods_type_lower in self._datatype_uuid_cache:
            return self._datatype_uuid_cache[ods_type_lower]
        
        # If not in cache (should not happen if prefetch worked correctly),
        # fetch it and add to cache
        import config
        
        # Get datatype path
        datatype_path = self._datatype_mapping.get(ods_type_lower)
        if not datatype_path:
            raise ValueError(f"Unknown ODS data type: {ods_type}")
        
        # Use the last part as the type name
        parts = datatype_path.strip('/').split('/')
        type_name = parts[-1]
        
        # Build path to datatype
        dtype_endpoint = f"/rest/{self.client.database_name}/schemes/{config.datatype_scheme_name}/datatypes/{type_name}"
        
        # Get datatype UUID
        response = self.client._get_asset(dtype_endpoint)
        if not response:
            raise ValueError(f"Could not find datatype for {ods_type}")
        
        # Cache the result
        uuid = response.get('id')
        self._datatype_uuid_cache[ods_type_lower] = uuid
        
        return uuid
