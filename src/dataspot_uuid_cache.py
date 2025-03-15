import csv
import os
import logging

# TODO: Test all aspects of this class; it is used in the DataspotClient class. COMPLETELY UNTESTED AS OF NOW!
class DataspotUUIDCache:
    """
    A cache for Dataspot UUIDs that uses a CSV file for storage.
    
    The cache stores mappings between asset types, names, UUIDs, and paths
    to reduce the number of API calls needed to retrieve UUIDs.
    """
    
    def __init__(self, csv_path="dataspot_uuids.csv"):
        """
        Initialize a UUID cache for Dataspot assets using a CSV file.
        
        Args:
            csv_path (str): Path to the CSV file. Default is "dataspot_uuids.csv".
        """
        self.csv_path = csv_path
        self.cache = {}
        
        # Load existing cache if file exists
        if os.path.exists(csv_path):
            try:
                with open(csv_path, 'r', newline='', encoding='utf-8') as csvfile:
                    reader = csv.DictReader(csvfile)
                    for row in reader:
                        if '_type' in row and 'name' in row and 'uuid' in row:
                            key = (row['_type'], row['name'])
                            self.cache[key] = {
                                'uuid': row['uuid'],
                                'path': row.get('path', '')
                            }
                logging.info(f"Loaded {len(self.cache)} entries from UUID cache at {csv_path}")
            except Exception as e:
                logging.warning(f"Could not load UUID cache from {csv_path}: {str(e)}")
                self.cache = {}
    
    def save_cache(self):
        """Save the cache to the CSV file."""
        try:
            # Create directory if it doesn't exist
            directory = os.path.dirname(self.csv_path)
            if directory and not os.path.exists(directory):
                os.makedirs(directory)
                
            # Convert cache to list of rows
            rows = []
            for (type_val, name), data in self.cache.items():
                rows.append({
                    '_type': type_val,
                    'uuid': data['uuid'],
                    'name': name,
                    'path': data.get('path', '')
                })
            
            # Write to CSV
            with open(self.csv_path, 'w', newline='', encoding='utf-8') as csvfile:
                fieldnames = ['_type', 'uuid', 'name', 'path']
                writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
                writer.writeheader()
                writer.writerows(rows)
            logging.debug(f"Saved {len(rows)} entries to UUID cache at {self.csv_path}")
        except Exception as e:
            logging.error(f"Failed to save UUID cache to {self.csv_path}: {str(e)}")
    
    def add_or_update_asset(self, _type, name, uuid, path=None):
        """
        Add or update an asset in the cache.
        
        Args:
            _type (str): The type of the asset (e.g., 'Dataset', 'Datatype')
            name (str): The name/label of the asset
            uuid (str): The UUID of the asset
            path (str, optional): The API path to access the asset
        """
        if not uuid:
            logging.warning(f"Attempted to cache asset with empty UUID: {_type}/{name}")
            return
            
        key = (_type, name)
        self.cache[key] = {
            'uuid': uuid,
            'path': path or ''
        }
        self.save_cache()
        logging.debug(f"Cached {_type} '{name}' with UUID {uuid}")
    
    def get_uuid(self, _type, name):
        """
        Get the UUID for an asset by type and name.
        
        Args:
            _type (str): The type of the asset
            name (str): The name/label of the asset
            
        Returns:
            str: The UUID if found, None otherwise
        """
        key = (_type, name)
        if key in self.cache:
            uuid = self.cache[key]['uuid']
            logging.debug(f"Cache hit for {_type} '{name}': UUID {uuid}")
            return uuid
        logging.debug(f"Cache miss for {_type} '{name}'")
        return None
    
    def get_path(self, _type, name):
        """
        Get the path for an asset by type and name.
        
        Args:
            _type (str): The type of the asset
            name (str): The name/label of the asset
            
        Returns:
            str: The path if found, None otherwise
        """
        key = (_type, name)
        if key in self.cache:
            path = self.cache[key]['path']
            logging.debug(f"Cache hit for {_type} '{name}': path {path}")
            return path
        logging.debug(f"Cache miss for {_type} '{name}'")
        return None
    
    def get_all_by_type(self, _type):
        """
        Get all assets of a specific type.
        
        Args:
            _type (str): The type of assets to retrieve
            
        Returns:
            list: A list of dictionaries with name, uuid, and path for each asset
        """
        results = []
        for (type_val, name), data in self.cache.items():
            if type_val == _type:
                results.append({
                    'name': name,
                    'uuid': data['uuid'],
                    'path': data.get('path', '')
                })
        return results
    
    def clear_cache(self):
        """Clear all entries in the cache."""
        self.cache = {}
        self.save_cache()
        logging.info(f"Cleared UUID cache at {self.csv_path}")
        
    def remove_asset(self, _type, name):
        """
        Remove an asset from the cache.
        
        Args:
            _type (str): The type of the asset
            name (str): The name/label of the asset
            
        Returns:
            bool: True if the asset was found and removed, False otherwise
        """
        key = (_type, name)
        if key in self.cache:
            del self.cache[key]
            self.save_cache()
            logging.debug(f"Removed {_type} '{name}' from UUID cache")
            return True
        return False 