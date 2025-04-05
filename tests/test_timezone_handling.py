import unittest
from src.dataset_transformer import iso_8601_to_unix_timestamp

class TestTimezoneHandling(unittest.TestCase):
    def test_utc_timezone_in_string(self):
        """Test ISO 8601 with explicit UTC timezone"""
        # 2025-03-07T00:00:00Z in UTC
        self.assertEqual(iso_8601_to_unix_timestamp("2025-03-07T00:00:00Z"), 1741305600000)
    
    def test_utc_timezone_without_z(self):
        """Test ISO 8601 with +00:00 UTC timezone"""
        # 2025-03-07T00:00:00+00:00 in UTC
        self.assertEqual(iso_8601_to_unix_timestamp("2025-03-07T00:00:00+00:00"), 1741305600000)
    
    def test_explicit_timezone_in_string(self):
        """Test ISO 8601 with explicit non-UTC timezone"""
        # 2025-03-07T00:00:00+01:00 in CET
        self.assertEqual(iso_8601_to_unix_timestamp("2025-03-07T00:00:00+01:00"), 1741302000000)
    
    def test_dataset_timezone_zurich(self):
        """Test dataset timezone when not in string"""
        # 2025-03-07T00:00:00 with Europe/Zurich timezone
        # During winter time, Zurich is UTC+1
        self.assertEqual(iso_8601_to_unix_timestamp("2025-03-07T00:00:00", "Europe/Zurich"), 1741302000000)
    
    def test_dataset_timezone_summer(self):
        """Test dataset timezone during summer time"""
        # 2025-07-07T00:00:00 with Europe/Zurich timezone
        # During summer time, Zurich is UTC+2
        self.assertEqual(iso_8601_to_unix_timestamp("2025-07-07T00:00:00", "Europe/Zurich"), 1751839200000)
    
    def test_no_timezone_info(self):
        """Test when no timezone is provided (should default to UTC)"""
        # 2025-03-07T00:00:00 with no timezone info (defaults to UTC)
        self.assertEqual(iso_8601_to_unix_timestamp("2025-03-07T00:00:00"), 1741305600000)
    
    def test_invalid_timezone(self):
        """Test with invalid timezone (should fall back to UTC)"""
        # Should fall back to UTC
        self.assertEqual(iso_8601_to_unix_timestamp("2025-03-07T00:00:00", "Invalid/Timezone"), 1741305600000)
    
    def test_missing_datetime(self):
        """Test with empty datetime string"""
        self.assertIsNone(iso_8601_to_unix_timestamp(None))
        self.assertIsNone(iso_8601_to_unix_timestamp(""))
    
    def test_malformed_datetime(self):
        """Test with malformed datetime string"""
        self.assertIsNone(iso_8601_to_unix_timestamp("not-a-date"))

if __name__ == '__main__':
    unittest.main() 