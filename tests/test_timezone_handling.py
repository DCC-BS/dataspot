import unittest
from src.clients.helpers import parse_iso_date_utc_midnight_ms

class TestPublicationDateHandling(unittest.TestCase):
    def test_date_only_string(self):
        """ODS dcat.issued as plain calendar date maps to UTC midnight of that day"""
        # 2019-11-05T00:00:00Z
        self.assertEqual(parse_iso_date_utc_midnight_ms("2019-11-05"), 1572912000000)

    def test_ignores_dataset_timezone_offset(self):
        """Calendar day is kept, independent of Europe/Zurich offset (no shift to previous day)"""
        self.assertNotEqual(parse_iso_date_utc_midnight_ms("2019-11-05"), 1572908400000)

    def test_longer_iso_string_uses_date_prefix(self):
        """Time and timezone parts are ignored, only the YYYY-MM-DD prefix counts"""
        midnight = parse_iso_date_utc_midnight_ms("2025-03-07")
        self.assertEqual(parse_iso_date_utc_midnight_ms("2025-03-07T00:00:00Z"), midnight)
        self.assertEqual(parse_iso_date_utc_midnight_ms("2025-03-07T08:30:45+01:00"), midnight)
        self.assertEqual(parse_iso_date_utc_midnight_ms("2025-03-07T23:59:59Z"), midnight)
        self.assertEqual(midnight, 1741305600000)

    def test_summer_date(self):
        """No daylight saving shift: summer dates also map to UTC midnight"""
        # 2025-07-07T00:00:00Z
        self.assertEqual(parse_iso_date_utc_midnight_ms("2025-07-07"), 1751846400000)

    def test_missing_date(self):
        self.assertIsNone(parse_iso_date_utc_midnight_ms(None))
        self.assertIsNone(parse_iso_date_utc_midnight_ms(""))

    def test_malformed_date(self):
        self.assertIsNone(parse_iso_date_utc_midnight_ms("not-a-date"))

if __name__ == '__main__':
    unittest.main()
