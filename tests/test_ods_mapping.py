import os
import unittest
from src.ods_dataspot_mapping import ODSDataspotMapping


class TestODSDataspotMapping(unittest.TestCase):
    """Test case for the ODSDataspotMapping class."""
    
    def setUp(self):
        # Use a temporary test file
        self.test_file = "test_mapping.csv"
        self.mapping = ODSDataspotMapping(self.test_file)
        
    def tearDown(self):
        # Clean up test file
        if os.path.exists(self.test_file):
            os.remove(self.test_file)
            
    def test_add_and_retrieve_entry(self):
        # Test adding and retrieving an entry
        self.mapping.add_entry("ods-123", "uuid-123", "http://example.com/123")
        
        # Retrieve the entry
        entry = self.mapping.get_entry("ods-123")
        self.assertEqual(entry, ("uuid-123", "http://example.com/123"))
        
        # Test convenience methods
        self.assertEqual(self.mapping.get_uuid("ods-123"), "uuid-123")
        self.assertEqual(self.mapping.get_href("ods-123"), "http://example.com/123")
        
    def test_remove_entry(self):
        # Add an entry
        self.mapping.add_entry("ods-456", "uuid-456", "http://example.com/456")
        
        # Remove it
        result = self.mapping.remove_entry("ods-456")
        self.assertTrue(result)
        
        # Check that it's gone
        self.assertIsNone(self.mapping.get_entry("ods-456"))
        
        # Try removing a non-existent entry
        result = self.mapping.remove_entry("nonexistent")
        self.assertFalse(result)
        
    def test_persistence(self):
        # Add entries
        self.mapping.add_entry("ods-789", "uuid-789", "http://example.com/789")
        self.mapping.add_entry("ods-abc", "uuid-abc", "http://example.com/abc")
        
        # Create a new instance that should load from the same file
        new_mapping = ODSDataspotMapping(self.test_file)
        
        # Check that entries were loaded
        self.assertEqual(new_mapping.get_uuid("ods-789"), "uuid-789")
        self.assertEqual(new_mapping.get_href("ods-abc"), "http://example.com/abc")


if __name__ == "__main__":
    unittest.main() 