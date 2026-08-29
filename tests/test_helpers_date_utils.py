#!/usr/bin/env python
"""
Test script for the UTC-midnight date helpers in src/clients/helpers.py
"""

import sys
import os
from datetime import date

import pytest

# Add the parent directory to the path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.clients.helpers import (
    coerce_utc_midnight_ms,
    date_to_utc_midnight_ms,
    format_utc_midnight_ms,
    parse_iso_date_utc_midnight_ms,
    parse_iso_date_ymd,
)


class TestParseIsoDateYmd:
    """Test cases for parse_iso_date_ymd."""

    def test_valid_ymd_string(self):
        assert parse_iso_date_ymd("2020-01-01") == date(2020, 1, 1)

    def test_valid_longer_iso_string(self):
        assert parse_iso_date_ymd("2020-01-01T00:00:00Z") == date(2020, 1, 1)

    def test_none(self):
        assert parse_iso_date_ymd(None) is None

    def test_empty_string(self):
        assert parse_iso_date_ymd("") is None

    def test_too_short_string(self):
        assert parse_iso_date_ymd("2020-01") is None

    def test_malformed_string(self):
        assert parse_iso_date_ymd("not-a-date") is None


class TestDateToUtcMidnightMs:
    """Test cases for date_to_utc_midnight_ms."""

    def test_epoch_boundary(self):
        assert date_to_utc_midnight_ms(date(1970, 1, 1)) == 0

    def test_normal_post_epoch_date(self):
        assert date_to_utc_midnight_ms(date(2025, 3, 7)) == 1741305600000

    def test_pre_epoch_negative_ms(self):
        # Dates before 1970-01-01 must yield a negative ms value.
        assert date_to_utc_midnight_ms(date(1930, 11, 3)) == -1235865600000


class TestParseIsoDateUtcMidnightMs:
    """Test cases for parse_iso_date_utc_midnight_ms."""

    def test_none(self):
        assert parse_iso_date_utc_midnight_ms(None) is None

    def test_empty_string(self):
        assert parse_iso_date_utc_midnight_ms("") is None

    def test_valid_date_string(self):
        assert parse_iso_date_utc_midnight_ms("2020-01-01") == 1577836800000

    def test_invalid_string(self):
        assert parse_iso_date_utc_midnight_ms("not-a-date") is None

    def test_pre_epoch_negative_ms_via_string(self):
        assert parse_iso_date_utc_midnight_ms("1930-11-03") == -1235865600000


class TestCoerceUtcMidnightMs:
    """Test cases for coerce_utc_midnight_ms."""

    def test_int_passthrough(self):
        assert coerce_utc_midnight_ms(1741305600000) == 1741305600000

    def test_numeric_string(self):
        assert coerce_utc_midnight_ms("1741305600000") == 1741305600000

    def test_none(self):
        assert coerce_utc_midnight_ms(None) is None

    def test_empty_string(self):
        assert coerce_utc_midnight_ms("") is None

    def test_non_numeric_string(self):
        assert coerce_utc_midnight_ms("not-a-number") is None


class TestFormatUtcMidnightMs:
    """Test cases for format_utc_midnight_ms."""

    def test_valid_ms(self):
        assert format_utc_midnight_ms(1741305600000) == "07.03.2025"

    def test_none(self):
        assert format_utc_midnight_ms(None) == ""

    def test_empty_string(self):
        assert format_utc_midnight_ms("") == ""

    def test_negative_ms_round_trip(self):
        # Pre-1970 dates produce negative ms; formatting must still round-trip correctly.
        assert format_utc_midnight_ms(-1235865600000) == "03.11.1930"


# This allows running the test file directly
if __name__ == "__main__":
    pytest.main(["-v", __file__])
