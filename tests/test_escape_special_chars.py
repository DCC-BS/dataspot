#!/usr/bin/env python
"""
Test script for the escape_special_chars function

This script tests the escape_special_chars function with various inputs, 
including examples from the Dataspot documentation.
"""

import sys
import os
import pytest

# Add the parent directory to the path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.clients.client_helpers import escape_special_chars


class TestEscapeSpecialChars:
    """Test cases for the escape_special_chars function."""
    
    # Test basic cases (no special characters)
    def test_basic_cases(self):
        assert escape_special_chars('Adresse') == 'Adresse'
        assert escape_special_chars('Person') == 'Person'
    
    # Test names with delimiters (/ or .)
    def test_names_with_delimiters(self):
        assert escape_special_chars('dataspot.') == '"dataspot."'
        assert escape_special_chars('INPUT/OUTPUT') == '"INPUT/OUTPUT"'
        assert escape_special_chars('Mitarbeiter.csv') == '"Mitarbeiter.csv"'
        assert escape_special_chars('Datei bzw. Schnittstelle') == '"Datei bzw. Schnittstelle"'
    
    # Test names with quotes that need doubling
    def test_names_with_quotes(self):
        assert escape_special_chars('28" City Bike') == '"28"" City Bike"'
        assert escape_special_chars('Projekt "Zeus"') == '"Projekt ""Zeus"""'
        assert escape_special_chars('Codename "Kronos"') == '"Codename ""Kronos"""'
    
    # Test combined cases from documentation
    def test_combined_cases(self):
        assert escape_special_chars('Technische Objekte.ABTEILUNG') == '"Technische Objekte.ABTEILUNG"'
        assert escape_special_chars('Datei bzw. Schnittstelle.Zeiterfassung') == '"Datei bzw. Schnittstelle.Zeiterfassung"'
        assert escape_special_chars('Datei bzw. Schnittstelle.Mitarbeiter.csv') == '"Datei bzw. Schnittstelle.Mitarbeiter.csv"'
    
    # Test edge cases
    def test_edge_cases(self):
        assert escape_special_chars("") == ""
        assert escape_special_chars(None) == None
    

# This allows running the test file directly
if __name__ == "__main__":
    # Run the tests with pytest
    pytest.main(["-v", __file__]) 