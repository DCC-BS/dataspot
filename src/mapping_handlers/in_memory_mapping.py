import uuid
import logging
from typing import Tuple, Optional, Dict, Callable

from src.clients.base_client import BaseDataspotClient


class InMemoryMapping:
    """
    In-memory mapping of external IDs to Dataspot asset type, UUID, and collection.
    Fetches mappings from Dataspot API when initialized, stores in memory (no CSV).
    """

    def __init__(self, database_name: str, id_field_name: str, scheme: str, client: BaseDataspotClient, 
                 filter_function: Optional[Callable] = None):
        """
        Initialize the mapping table by fetching from Dataspot API.
        
        Args:
            database_name (str): Name of the database
            id_field_name (str): Name of the ID field (e.g., 'odsDataportalId', 'staatskalender_id')
            scheme (str): Name of the scheme (e.g., 'DNK', 'TDM')
            client (BaseDataspotClient): Client instance to use for API operations
            filter_function (Callable, optional): Function to filter assets. If None, uses simple ID field check.
        """
        self._id_field_name = id_field_name
        self._scheme = scheme
        self.database_name = database_name
        
        if not database_name:
            raise ValueError("database_name cannot be empty")
        if not scheme:
            raise ValueError("scheme cannot be empty")

        # Mapping: Dict[str, Tuple[str, str, Optional[str]]] -> Dict[external_id, (_type, uuid, inCollection)]
        self.mapping: Dict[str, Tuple[str, str, Optional[str]]] = {}
        
        # Fetch mappings from Dataspot API
        self._fetch_mappings_from_api(client, filter_function)
    
    @property
    def id_field_name(self) -> str:
        """Get the field name for the ID in this mapping"""
        return self._id_field_name

    def _fetch_mappings_from_api(self, client: BaseDataspotClient, filter_function: Optional[Callable] = None) -> None:
        """
        Fetch mappings from Dataspot API and populate the in-memory mapping dict.
        
        Args:
            client: BaseDataspotClient instance to use for API operations
            filter_function: Optional function to filter assets
        """
        logging.info(f"Fetching mappings from Dataspot API for scheme '{self._scheme}' (field: {self._id_field_name})")
        
        try:
            # Get all assets from the scheme
            assets = client.get_all_assets_from_scheme()
            
            if not assets:
                logging.warning(f"No assets found in scheme '{self._scheme}'")
                return
            
            # Apply filter if provided, otherwise use simple ID field check
            if filter_function:
                assets = [asset for asset in assets if filter_function(asset)]
            else:
                # Default filter: asset must have the ID field
                assets = [asset for asset in assets if asset.get(self._id_field_name) is not None]
            
            # Build mapping dict
            mapping_count = 0
            for asset in assets:
                id_value = asset.get(self._id_field_name)
                if id_value is not None:
                    id_str = str(id_value)
                    uuid_val = asset.get('id')
                    _type = asset.get('_type')
                    inCollection = asset.get('inCollection')
                    
                    if uuid_val and _type:
                        self.mapping[id_str] = (_type, uuid_val, inCollection)
                        mapping_count += 1
                    else:
                        logging.debug(f"Asset with {self._id_field_name}={id_value} missing UUID or _type, skipping")
            
            logging.info(f"Fetched {mapping_count} mappings from Dataspot API for scheme '{self._scheme}'")
            
        except Exception as e:
            logging.error(f"Error fetching mappings from Dataspot API: {str(e)}")
            logging.warning("Continuing with empty mapping - mappings will be populated as assets are created/updated")
            # Continue with empty mapping - it will be populated as we work

    def _is_valid_uuid(self, uuid_str: str) -> bool:
        """
        Check if the string is a valid UUID format.

        Args:
            uuid_str (str): The UUID string to validate

        Returns:
            bool: True if the UUID is valid, False otherwise
        """
        try:
            # Try to parse it as a UUID
            uuid_obj = uuid.UUID(uuid_str)
            return str(uuid_obj) == uuid_str
        except (ValueError, AttributeError):
            return False

    def get_entry(self, external_id: str) -> Optional[Tuple[str, str, Optional[str]]]:
        """
        Get the _type, UUID, and inCollection for an external ID if it exists.

        Args:
            external_id (str): The external ID to look up

        Returns:
            Optional[Tuple[str, str, Optional[str]]]: A tuple of (_type, uuid, inCollection) if found, None otherwise
        """
        return self.mapping.get(external_id)

    def add_entry(self, external_id: str, _type: str, uuid_str: str, in_collection: Optional[str] = None) -> bool:
        """
        Add a new mapping entry or update an existing one.

        Args:
            external_id (str): The external ID
            _type (str): The Dataspot asset type (e.g., "Dataset", "Collection")
            uuid_str (str): The Dataspot UUID
            in_collection (str, optional): The business key of the collection containing this asset. Defaults to None.

        Returns:
            bool: True if the entry was added successfully, False otherwise
        """
        # Check for empty required values with specific error messages
        empty_params = []
        if not external_id:
            empty_params.append("external_id")
        if not _type:
            empty_params.append("_type")
        if not uuid_str:
            empty_params.append("uuid_str")

        if empty_params:
            logging.warning("Cannot add entry with empty values for: %s", ", ".join(empty_params))
            logging.warning("Provided values - %s: '%s', _type: '%s', uuid_str: '%s'",
                               self.id_field_name, external_id, _type, uuid_str)
            return False

        # Validate UUID format
        if not self._is_valid_uuid(uuid_str):
            logging.warning("Invalid UUID format: '%s' for %s '%s'", uuid_str, self.id_field_name, external_id)
            logging.warning("UUID must match the format: 'xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx' (8-4-4-4-12 hex digits)")
            return False

        # Store the entry including _type (no CSV save)
        self.mapping[external_id] = (_type, uuid_str, in_collection)
        return True

    def remove_entry(self, external_id: str) -> bool:
        """
        Remove a mapping entry if it exists.

        Args:
            external_id (str): The external ID to remove

        Returns:
            bool: True if the entry was removed, False if it didn't exist
        """
        if external_id in self.mapping:
            del self.mapping[external_id]
            return True
        return False

    def get_inCollection(self, external_id: str) -> Optional[str]:
        """
        Get just the inCollection business key for an external ID.

        Args:
            external_id (str): The external ID to look up

        Returns:
            Optional[str]: The inCollection business key if found, None otherwise
        """
        entry = self.get_entry(external_id)
        return entry[2] if entry else None  # Index 2 for inCollection
