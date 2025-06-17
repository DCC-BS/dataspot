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
            "stereotype": "ogd_dataset_component",
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
            asset.get('customProperties', {}).get('ODS_ID') == ods_id
        )
        
        existing_assets = self.get_all_assets_from_scheme(filter_function=asset_filter)
        
        if existing_assets:
            # Asset exists, update it
            is_new = False
            asset = existing_assets[0]
            asset_uuid = asset.get('id')
            
            if not asset_uuid:
                raise ValueError(f"Found existing dataobject for {ods_id} but could not get UUID")
                
            # Update the existing dataobject
            endpoint = f"/rest/{self.database_name}/assets/{asset_uuid}"
            response = self._update_asset(endpoint=endpoint, data=dataobject, replace=False)
        else:
            # Create new dataobject in the ODS-Imports collection
            collection_uuid = collection_data.get('id')
            if not collection_uuid:
                raise ValueError("Failed to get collection UUID")
            
            # Important: Do NOT set inCollection when using collection-specific endpoint
            # The collection is implicitly set by the endpoint we're using
            
            # Create the dataobject
            endpoint = f"/rest/{self.database_name}/collections/{collection_uuid}/assets"
            response = self._create_asset(endpoint=endpoint, data=dataobject)
            asset_uuid = response.get('id')
            
            if not asset_uuid:
                raise ValueError(f"Failed to create dataobject for {ods_id}")
        
        # Process attributes (columns)
        attributes_endpoint = f"/rest/{self.database_name}/assets/{asset_uuid}/attributes"
        
        # Get existing attributes to determine what to update vs create
        existing_attributes = {}
        
        try:
            attrs_response = self._get_asset(attributes_endpoint)
            if attrs_response and '_embedded' in attrs_response and 'attributes' in attrs_response['_embedded']:
                for attr in attrs_response['_embedded']['attributes']:
                    existing_attributes[attr['label']] = attr
        except Exception:
            # Continue if there was an error getting attributes
            pass
        
        # Track changes
        created_attrs = []
        updated_attrs = []
        unchanged_attrs = []
        deleted_attrs = []
        
        # Process each column as an attribute
        for column in columns:
            # Map ODS types to UML data types
            datatype_uuid = self._get_datatype_uuid(column['type'])
            
            attribute = {
                "_type": "UmlAttribute",
                "title": column['label'],
                "label": column['name'],
                "hasRange": datatype_uuid
            }
            
            # Check if attribute exists to determine if update or create
            if column['name'] in existing_attributes:
                # Get existing attribute data
                existing_attr = existing_attributes[column['name']]
                attr_uuid = existing_attr.get('id')
                
                # Check if anything changed
                if (existing_attr.get('title') == column['label'] and
                    existing_attr.get('hasRange') == datatype_uuid):
                    # Attribute is unchanged
                    unchanged_attrs.append(column['name'])
                else:
                    # Update the attribute
                    attr_endpoint = f"/rest/{self.database_name}/attributes/{attr_uuid}"
                    self._update_asset(endpoint=attr_endpoint, data=attribute, replace=False)
                    updated_attrs.append(column['name'])
                
                # Remove from existing_attributes to track what's left for deletion
                del existing_attributes[column['name']]
            else:
                # Create new attribute
                self._create_asset(endpoint=attributes_endpoint, data=attribute)
                created_attrs.append(column['name'])
        
        # Handle deletions - any attributes still in existing_attributes need to be removed
        for attr_name, attr_data in existing_attributes.items():
            attr_uuid = attr_data.get('id')
            if attr_uuid:
                attr_endpoint = f"/rest/{self.database_name}/attributes/{attr_uuid}"
                self._delete_asset(attr_endpoint)
                deleted_attrs.append(attr_name)
        
        # Prepare result
        result = {
            "status": "success",
            "message": f"{'Created new' if is_new else 'Updated existing'} dataobject for dataset {ods_id}",
            "uuid": asset_uuid,
            "counts": {
                "created_attributes": len(created_attrs),
                "updated_attributes": len(updated_attrs),
                "unchanged_attributes": len(unchanged_attrs),
                "deleted_attributes": len(deleted_attrs)
            },
            "details": {
                "created_attributes": created_attrs,
                "updated_attributes": updated_attrs,
                "unchanged_attributes": unchanged_attrs,
                "deleted_attributes": deleted_attrs
            }
        }
        
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
            'text': '/Datentypmodell/Zeichenkette',
            'int': '/Datentypmodell/Ganzzahl',
            'boolean': '/Datentypmodell/Wahrheitswert',
            'double': '/Datentypmodell/Dezimalzahl',
            'datetime': '/Datentypmodell/Zeitpunkt',
            'date': '/Datentypmodell/Datum',
            'geo_point_2d': '/Datentypmodell/geo_point_2d',
            'geo_shape': '/Datentypmodell/geo_shape',
            'file': '/Datentypmodell/Binärdaten',
            'json_blob': '/Datentypmodell/Zeichenkette',
            'identifier': '/Datentypmodell/Identifier'
        }
        
        # Get datatype path
        datatype_path = type_mapping.get(ods_type.lower(), '/Datentypmodell/Zeichenkette')
        
        # Use the last part as the type name
        parts = datatype_path.split('/')
        type_name = parts[-1]
        
        # Build path to datatype
        dtype_endpoint = f"/rest/{self.database_name}/schemes/{config.datatype_scheme_name}/datatypes/{type_name}"
        
        # Get datatype UUID
        response = self._get_asset(dtype_endpoint)
        if not response:
            # Default to Zeichenkette (string) if type not found
            dtype_endpoint = f"/rest/{self.database_name}/schemes/{config.datatype_scheme_name}/datatypes/Zeichenkette"
            response = self._get_asset(dtype_endpoint)
            
            if not response:
                raise ValueError(f"Could not find datatype for {ods_type}")
        
        return response.get('id')
