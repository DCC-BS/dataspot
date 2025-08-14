import os
import csv
import uuid
import logging
from typing import Tuple, Optional, Dict, List


class BaseDataspotMapping:
    """
    Base class for mapping external IDs to Dataspot asset type, UUID, and collection.
    This class provides functionality to store and retrieve mappings between external IDs
    and Dataspot assets (type, UUID, and collection).
    """

    def __init__(self, database_name: str, id_field_name: str, file_prefix: str, scheme: str):
        """
        Initialize the mapping table.
        
        Args:
            database_name (str): Name of the database to use for file naming.
            id_field_name (str): Name of the ID field (e.g., 'odsDataportalId', 'staatskalender_id')
            file_prefix (str): Prefix for the mapping file (e.g., 'ods-dataspot', 'staatskalender-dataspot')
            scheme (str): Name of the scheme (e.g., 'DNK', 'TDM')
        """
        self._id_field_name = id_field_name
        self._file_prefix = file_prefix
        self._scheme = scheme

        if not database_name:
            raise ValueError("database_name cannot be empty")
        if not scheme:
            raise ValueError("scheme cannot be empty")

        # Store database name for endpoint construction
        self.database_name = database_name

        # Derive csv_file_path from the mandatory database_name and scheme
        self.csv_file_path = self._get_mapping_file_path(database_name, scheme)
        
        # Ensure the mappings directory exists
        mappings_dir = os.path.dirname(self.csv_file_path)
        if not os.path.exists(mappings_dir):
            try:
                os.makedirs(mappings_dir)
                logging.info(f"Created mappings directory: {mappings_dir}")
            except (IOError, PermissionError) as e:
                logging.warning(f"Could not create mappings directory: {str(e)}")
        
        logging.info(f"Using mapping file: {self.csv_file_path}")

        # Mapping: Dict[str, Tuple[str, str, Optional[str]]] -> Dict[external_id, (_type, uuid, inCollection)]
        self.mapping: Dict[str, Tuple[str, str, Optional[str]]] = {}
        self._load_mapping()

    @property
    def id_field_name(self) -> str:
        """Get the field name for the ID in this mapping"""
        return self._id_field_name

    @property
    def csv_headers(self) -> List[str]:
        """Get the CSV headers for this mapping"""
        return [self.id_field_name, '_type', 'uuid', 'inCollection']

    def _get_mapping_file_path(self, database_name: str, scheme: str) -> str:
        """Get the file path for the mapping file based on database name and scheme. Format: mappings/{database_name}_{scheme}_{file_prefix}-mapping.csv"""
        # Get project root directory (2 levels up from this module)
        current_file_path = os.path.abspath(__file__)
        src_dir = os.path.dirname(os.path.dirname(current_file_path))
        project_root = os.path.dirname(src_dir)
        
        # Define mappings directory
        mappings_dir = os.path.join(project_root, "mappings")
        
        # Construct the filename
        filename = f"{database_name}_{scheme}_{self._file_prefix}-mapping.csv"
        
        # Return the full path
        return os.path.join(mappings_dir, filename)

    def _load_mapping(self) -> None:
        """Load the mapping from the CSV file if it exists."""
        if not os.path.exists(self.csv_file_path):
            # Create the file with headers if it doesn't exist
            try:
                with open(self.csv_file_path, 'w', newline='', encoding='utf-8') as csvfile:
                    writer = csv.writer(csvfile)
                    writer.writerow(self.csv_headers)  # Use defined headers
            except (IOError, PermissionError) as e:
                logging.warning(f"Could not create mapping file: %s", str(e))
            return

        try:
            with open(self.csv_file_path, 'r', newline='', encoding='utf-8') as csvfile:
                reader = csv.DictReader(csvfile)
                # Check headers
                if reader.fieldnames != self.csv_headers:
                    logging.warning(f"CSV file header mismatch in {self.csv_file_path}. "
                                        f"Expected: {self.csv_headers}, Found: {reader.fieldnames}. "
                                        f"Attempting to load anyway, but data might be misinterpreted or fail.")

                self.mapping = {}  # Clear existing mapping before loading
                for row in reader:
                    # Check if required columns exist in the row
                    # 'inCollection' is optional in the data, but the column must exist
                    required_fields = [self.id_field_name, '_type', 'uuid']
                    if not all(h in row for h in required_fields):
                        logging.warning(f"Skipping row due to missing required columns ({', '.join(required_fields)}): {row}")
                        continue

                    external_id = row[self.id_field_name]
                    _type = row['_type']
                    uuid_val = row['uuid']
                    # Handle potentially missing or empty inCollection
                    inCollection = row.get('inCollection')
                    if inCollection == '':
                        inCollection = None

                    self.mapping[external_id] = (_type, uuid_val, inCollection)

        except (IOError, PermissionError) as e:
            logging.warning(f"Could not read mapping file: %s", str(e))
        except KeyError as e:
            logging.warning(f"Missing expected column '{e}' while parsing row in {self.csv_file_path}. Check header format.")
        except Exception as e:
            logging.warning(f"Error parsing mapping file: %s", str(e))

    def save_to_csv(self) -> None:
        """Save the current mapping to the CSV file."""
        try:
            # Sort the items by external_id
            sorted_mapping = sorted(self.mapping.items(), key=lambda x: x[0])
            with open(self.csv_file_path, 'w', newline='', encoding='utf-8') as csvfile:
                writer = csv.writer(csvfile)
                writer.writerow(self.csv_headers)  # Write the headers
                for external_id, (_type, uuid_val, inCollection) in sorted_mapping:
                    # Ensure inCollection is written as empty string if None
                    writer.writerow([external_id, _type, uuid_val, inCollection or ''])
        except (IOError, PermissionError) as e:
            logging.warning(f"Could not write to mapping file: %s", str(e))

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

        # Store the entry including _type
        self.mapping[external_id] = (_type, uuid_str, in_collection)
        self.save_to_csv()
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
            self.save_to_csv()
            return True
        return False

    def get_type(self, external_id: str) -> Optional[str]:
        """
        Get just the _type for an external ID.

        Args:
            external_id (str): The external ID to look up

        Returns:
            Optional[str]: The asset _type if found, None otherwise
        """
        entry = self.get_entry(external_id)
        return entry[0] if entry else None  # Index 0 for _type

    def get_uuid(self, external_id: str) -> Optional[str]:
        """
        Get just the UUID for an external ID.

        Args:
            external_id (str): The external ID to look up

        Returns:
            Optional[str]: The UUID if found, None otherwise
        """
        entry = self.get_entry(external_id)
        return entry[1] if entry else None  # Index 1 for UUID

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

    def get_all_entries(self) -> Dict[str, Tuple[str, str, Optional[str]]]:
        """
        Get all mapping entries.

        Returns:
            Dict[str, Tuple[str, str, Optional[str]]]: Dictionary of all external ID to (_type, UUID, inCollection) mappings
        """
        return dict(self.mapping)

    def get_all_ids(self) -> List[str]:
        """
        Get a list of all external IDs in the mapping.

        Returns:
            List[str]: A list of all external IDs
        """
        return list(self.mapping.keys()) 