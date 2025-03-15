import unittest
import os
import tempfile
import csv
import logging
from unittest.mock import patch, mock_open
from src.dataspot_uuid_cache import DataspotUUIDCache

class TestDataspotUUIDCache(unittest.TestCase):
    def setUp(self):
        # Create a temporary directory for test files
        self.temp_dir = tempfile.TemporaryDirectory()
        self.test_csv_path = os.path.join(self.temp_dir.name, "test_cache.csv")
        
        # Set up test data
        self.test_data = [
            {"_type": "Dataset", "name": "Test Dataset", "uuid": "123-456", "path": "/datasets/123-456"},
            {"_type": "Datatype", "name": "Test Datatype", "uuid": "789-012", "path": "/datatypes/789-012"}
        ]
        
        # Create a test CSV file
        with open(self.test_csv_path, 'w', newline='', encoding='utf-8') as csvfile:
            fieldnames = ['_type', 'uuid', 'name', 'path']
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(self.test_data)
    
    def tearDown(self):
        # Clean up temporary directory
        self.temp_dir.cleanup()
    
    def test_init_with_existing_file(self):
        """Test initialization with an existing CSV file"""
        cache = DataspotUUIDCache(self.test_csv_path)
        
        # Verify cache is loaded correctly
        self.assertEqual(len(cache.cache), 2)
        self.assertEqual(cache.get_uuid("Dataset", "Test Dataset"), "123-456")
        self.assertEqual(cache.get_uuid("Datatype", "Test Datatype"), "789-012")
    
    def test_init_with_nonexistent_file(self):
        """Test initialization with a non-existent CSV file"""
        nonexistent_path = os.path.join(self.temp_dir.name, "nonexistent.csv")
        cache = DataspotUUIDCache(nonexistent_path)
        
        # Verify cache is empty
        self.assertEqual(len(cache.cache), 0)
    
    def test_init_with_corrupted_file(self):
        """Test initialization with a corrupted CSV file"""
        corrupted_path = os.path.join(self.temp_dir.name, "corrupted.csv")
        
        # Create a corrupted CSV file
        with open(corrupted_path, 'w', encoding='utf-8') as f:
            f.write("This is not a valid CSV file")
        
        # Should handle the exception and initialize with an empty cache
        cache = DataspotUUIDCache(corrupted_path)
        self.assertEqual(len(cache.cache), 0)
    
    def test_add_or_update_asset_new(self):
        """Test adding a new asset"""
        cache = DataspotUUIDCache(self.test_csv_path)
        original_count = len(cache.cache)
        
        # Add a new asset
        cache.add_or_update_asset("API", "Test API", "345-678", "/apis/345-678")
        
        # Verify it was added
        self.assertEqual(len(cache.cache), original_count + 1)
        self.assertEqual(cache.get_uuid("API", "Test API"), "345-678")
        self.assertEqual(cache.get_path("API", "Test API"), "/apis/345-678")
    
    def test_add_or_update_asset_existing(self):
        """Test updating an existing asset"""
        cache = DataspotUUIDCache(self.test_csv_path)
        original_count = len(cache.cache)
        
        # Update an existing asset
        cache.add_or_update_asset("Dataset", "Test Dataset", "updated-uuid", "/datasets/updated")
        
        # Verify it was updated without adding a new entry
        self.assertEqual(len(cache.cache), original_count)
        self.assertEqual(cache.get_uuid("Dataset", "Test Dataset"), "updated-uuid")
        self.assertEqual(cache.get_path("Dataset", "Test Dataset"), "/datasets/updated")
    
    def test_add_asset_without_path(self):
        """Test adding an asset without a path"""
        cache = DataspotUUIDCache(self.test_csv_path)
        
        cache.add_or_update_asset("Report", "Test Report", "567-890")
        
        self.assertEqual(cache.get_uuid("Report", "Test Report"), "567-890")
        self.assertEqual(cache.get_path("Report", "Test Report"), "")
    
    def test_add_asset_with_empty_uuid(self):
        """Test adding an asset with an empty UUID"""
        cache = DataspotUUIDCache(self.test_csv_path)
        original_count = len(cache.cache)
        
        cache.add_or_update_asset("Report", "Empty UUID", "", "/reports/empty")
        
        # Should not add to cache when UUID is empty
        self.assertEqual(len(cache.cache), original_count)
        self.assertIsNone(cache.get_uuid("Report", "Empty UUID"))
    
    def test_get_uuid_existing(self):
        """Test getting UUID for an existing asset"""
        cache = DataspotUUIDCache(self.test_csv_path)
        
        uuid = cache.get_uuid("Dataset", "Test Dataset")
        self.assertEqual(uuid, "123-456")
    
    def test_get_uuid_nonexistent(self):
        """Test getting UUID for a non-existent asset"""
        cache = DataspotUUIDCache(self.test_csv_path)
        
        uuid = cache.get_uuid("NonExistent", "Does Not Exist")
        self.assertIsNone(uuid)
    
    def test_get_path_existing(self):
        """Test getting path for an existing asset"""
        cache = DataspotUUIDCache(self.test_csv_path)
        
        path = cache.get_path("Dataset", "Test Dataset")
        self.assertEqual(path, "/datasets/123-456")
    
    def test_get_path_nonexistent(self):
        """Test getting path for a non-existent asset"""
        cache = DataspotUUIDCache(self.test_csv_path)
        
        path = cache.get_path("NonExistent", "Does Not Exist")
        self.assertIsNone(path)
    
    def test_get_all_by_type_existing(self):
        """Test getting all assets of an existing type"""
        cache = DataspotUUIDCache(self.test_csv_path)
        
        # Add another dataset to have multiple results
        cache.add_or_update_asset("Dataset", "Another Dataset", "234-567", "/datasets/234-567")
        
        results = cache.get_all_by_type("Dataset")
        self.assertEqual(len(results), 2)
        
        # Check that both datasets are included in the results
        dataset_names = [r["name"] for r in results]
        self.assertIn("Test Dataset", dataset_names)
        self.assertIn("Another Dataset", dataset_names)
    
    def test_get_all_by_type_nonexistent(self):
        """Test getting all assets of a non-existent type"""
        cache = DataspotUUIDCache(self.test_csv_path)
        
        results = cache.get_all_by_type("NonExistent")
        self.assertEqual(results, [])
    
    def test_clear_cache(self):
        """Test clearing the cache"""
        cache = DataspotUUIDCache(self.test_csv_path)
        self.assertGreater(len(cache.cache), 0)  # Verify cache is not empty
        
        cache.clear_cache()
        
        # Verify cache is now empty
        self.assertEqual(len(cache.cache), 0)
        self.assertIsNone(cache.get_uuid("Dataset", "Test Dataset"))
        
        # Check that the file was updated
        with open(self.test_csv_path, 'r', newline='', encoding='utf-8') as csvfile:
            reader = csv.DictReader(csvfile)
            rows = list(reader)
            self.assertEqual(len(rows), 0)
    
    def test_remove_asset_existing(self):
        """Test removing an existing asset"""
        cache = DataspotUUIDCache(self.test_csv_path)
        original_count = len(cache.cache)
        
        result = cache.remove_asset("Dataset", "Test Dataset")
        
        # Verify it was removed
        self.assertTrue(result)
        self.assertEqual(len(cache.cache), original_count - 1)
        self.assertIsNone(cache.get_uuid("Dataset", "Test Dataset"))
    
    def test_remove_asset_nonexistent(self):
        """Test removing a non-existent asset"""
        cache = DataspotUUIDCache(self.test_csv_path)
        original_count = len(cache.cache)
        
        result = cache.remove_asset("NonExistent", "Does Not Exist")
        
        # Verify nothing was removed
        self.assertFalse(result)
        self.assertEqual(len(cache.cache), original_count)
    
    @patch('os.makedirs')
    def test_save_cache_create_directory(self, mock_makedirs):
        """Test saving cache creates directory if needed"""
        nested_path = os.path.join(self.temp_dir.name, "nested", "dir", "cache.csv")
        cache = DataspotUUIDCache(nested_path)
        
        # Add an asset to trigger saving
        cache.add_or_update_asset("Test", "Item", "uuid-123")
        
        # Verify directory creation was attempted
        mock_makedirs.assert_called()
    
    @patch('builtins.open', side_effect=IOError("Simulated IO error"))
    def test_save_cache_io_error(self, mock_open):
        """Test handling of IO error when saving cache"""
        cache = DataspotUUIDCache(self.test_csv_path)
        
        # This should not raise an exception despite the IO error
        cache.add_or_update_asset("Test", "IO Error", "uuid-123")
        
        # Verify open was called
        mock_open.assert_called()

if __name__ == '__main__':
    unittest.main() 