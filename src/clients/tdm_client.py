import logging
from typing import Dict, Any, List

import config
from src.clients.base_client import BaseDataspotClient
from src.mapping_handlers.org_structure_handler import OrgStructureHandler
from src.ods_client import ODSClient

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

    # Synchronization methods
    def sync_org_units(self, org_data: Dict[str, Any], status: str = "WORKING") -> Dict[str, Any]:
        """
        Synchronize organizational units in Dataspot with data from the Staatskalender ODS API.

        Args:
            org_data: Dictionary containing organization data from ODS API
            status: Status to set on updated org units. Defaults to "WORKING" (DRAFT group).
                   Use "PUBLISHED" to make updates public immediately.
            
        Returns:
            Dict: Summary of the synchronization process
        """
        return self.org_handler.sync_org_units(org_data, status=status)
        
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
        # Ensure collection exists
        collection_data = self.ensure_ods_imports_collection_exists()

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
        
        # Check if asset already exists with this ODS_ID
        asset_filter = lambda asset: (
            asset.get('_type') == 'UmlClass' and
            asset.get('stereotype') == 'ogd_dataset' and
            asset.get('ODS_ID') == ods_id
        )
        
        logging.info(f"Checking if dataobject for dataset {ods_id} already exists...")
        existing_assets = self.get_all_assets_from_scheme(filter_function=asset_filter)

        if existing_assets:
            if len(existing_assets) > 1:
                logging.error(f"Found {len(existing_assets)} assets with ods_id {ods_id} in the {self.scheme_name_short} when only one should exist!")
                raise
            else:
                logging.info(f"Found existing dataobject for dataset {ods_id} (UUID: {existing_assets[0].get('id')})")

            # Asset exists, update it
            is_new = False
            asset = existing_assets[0]
            asset_uuid = asset.get('id')
            
            if not asset_uuid:
                raise ValueError(f"Found existing dataobject for {ods_id} but could not get UUID")
                
            # Update the existing dataobject
            endpoint = f"/rest/{self.database_name}/assets/{asset_uuid}"
            logging.info(f"Updating existing dataobject properties for dataset {ods_id}...")
            response = self._update_asset(endpoint=endpoint, data=dataobject, replace=False)
        else:
            # Create new dataobject in the ODS-Imports collection
            collection_uuid = collection_data.get('id')
            if not collection_uuid:
                raise ValueError("Failed to get collection UUID")
            
            # Important: Do NOT set inCollection when using collection-specific endpoint
            # The collection is implicitly set by the endpoint we're using
            
            # Create the dataobject
            logging.info(f"Creating new dataobject for dataset {ods_id} in collection {self.ods_imports_collection_name}...")
            endpoint = f"/rest/{self.database_name}/collections/{collection_uuid}/assets"
            response = self._create_asset(endpoint=endpoint, data=dataobject)
            asset_uuid = response.get('id')
            
            if not asset_uuid:
                raise ValueError(f"Failed to create dataobject for {ods_id}")
            else:
                logging.info(f"Successfully created new dataobject (UUID: {asset_uuid})")
        
        # Process attributes (columns)
        attributes_endpoint = f"/rest/{self.database_name}/classifiers/{asset_uuid}/attributes"
        
        # Get existing attributes to determine what to update vs create
        existing_attributes = {}
        
        try:
            logging.info(f"Retrieving existing attributes for dataset {ods_id}...")
            attrs_response = self._get_asset(attributes_endpoint)
            if attrs_response and '_embedded' in attrs_response and 'attributes' in attrs_response['_embedded']:
                for attr in attrs_response['_embedded']['attributes']:
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
            if 'description' in column and column['description']:
                attribute['description'] = column['description']
            
            # Check if attribute exists to determine if update or create
            if column['name'] in existing_attributes:
                # Get existing attribute data
                existing_attr = existing_attributes[column['name']]
                attr_uuid = existing_attr.get('id')
                
                # Check if anything changed
                if (existing_attr.get('label') == column['label'] and
                    existing_attr.get('hasRange') == datatype_uuid and
                    existing_attr.get('description') == column.get('description')):
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
                    if 'description' in attr_changes:
                        changes_desc.append(f"description changed")
                    
                    logging.info(f"Updating attribute '{column['name']}': {', '.join(changes_desc)}")
                    
                    # Update the attribute
                    attr_endpoint = f"/rest/{self.database_name}/attributes/{attr_uuid}"
                    self._update_asset(endpoint=attr_endpoint, data=attribute, replace=False)
                    updated_attrs.append(column['name'])
                
                # Remove from existing_attributes to track what's left for deletion
                del existing_attributes[column['name']]
            else:
                # Create new attribute
                logging.info(f"Creating new attribute '{column['name']}' with type '{column['type']}'")
                self._create_asset(endpoint=attributes_endpoint, data=attribute)
                created_attrs.append(column['name'])
        
        # Handle deletions - any attributes still in existing_attributes need to be removed
        if existing_attributes:
            logging.info(f"Found {len(existing_attributes)} attributes to delete")
            
        for attr_name, attr_data in existing_attributes.items():
            attr_uuid = attr_data.get('id')
            if attr_uuid:
                logging.info(f"Deleting unused attribute '{attr_name}'")
                attr_endpoint = f"/rest/{self.database_name}/attributes/{attr_uuid}"
                self._delete_asset(attr_endpoint)
                deleted_attrs.append(attr_name)
        
        # Asset link for reference in results
        dataspot_link = f"{self.base_url}/web/{self.database_name}/assets/{asset_uuid}" if asset_uuid else ""
        
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
        
        return result
    
    def _get_datatype_uuid(self, ods_type: str) -> str:
        """
        Map ODS data type to Dataspot datatype UUID.
        
        Args:
            ods_type (str): The ODS data type
            
        Returns:
            str: UUID of the corresponding datatype in Dataspot
        """
        # Map ODS types to UML data types
        type_mapping = {
            'text': '/Datentypmodell/Text',
            'int': '/Datentypmodell/Ganzzahl',
            'boolean': '/Datentypmodell/Wahrheitswert',
            'double': '/Datentypmodell/Dezimalzahl',
            'datetime': '/Datentypmodell/Zeitpunkt',
            'date': '/Datentypmodell/Datum',
            'geo_point_2d': '/Datentypmodell/geo_point_2d',
            'geo_shape': '/Datentypmodell/geo_shape',
            'file': '/Datentypmodell/Binärdaten',
            'json_blob': '/Datentypmodell/Text',
            'identifier': '/Datentypmodell/Identifier'
        }
        
        # Get datatype path
        datatype_path = type_mapping[ods_type.lower()]
        
        # Use the last part as the type name
        parts = datatype_path.strip('/').split('/')
        type_name = parts[-1]
        
        # Build path to datatype
        dtype_endpoint = f"/rest/{self.database_name}/schemes/{config.datatype_scheme_name}/datatypes/{type_name}"
        
        # Get datatype UUID
        response = self._get_asset(dtype_endpoint)
        if not response:
            raise ValueError(f"Could not find datatype for {ods_type}")
        
        return response.get('id')
