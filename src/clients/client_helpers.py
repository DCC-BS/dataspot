"""
Dataspot API Client Helpers

This module provides utility functions and helpers for Dataspot API clients.
"""

import logging


def url_join(*parts: str) -> str:
    """
    Join URL parts ensuring proper formatting with slashes.
    
    Args:
        *parts: URL parts to be joined.
        
    Returns:
        str: A properly formatted URL.
    """
    return "/".join([part.strip("/") for part in parts])


def create_url_to_website(path: str) -> str:
    """
    Create a URL to the Staatskalender website from a path.
    
    Args:
        path (str): The organization path
        
    Returns:
        str: The URL to the Staatskalender website
    """
    # Make path string lowercase, and replace ö -> oe, ä -> ae, ü -> ue, space -> '-', and remove trailing slashes
    new_path = path.lower()
    new_path = new_path.replace(' ', '-')
    new_path = new_path.replace('ö', 'oe')
    new_path = new_path.replace('ä', 'ae')
    new_path = new_path.replace('ü', 'ue')
    # Keep only letters a-z and hyphens, remove all other characters
    new_path = ''.join(c for c in new_path if c.isalpha() or c in '-/')
    new_path = new_path.replace('--', '-')
    new_path = new_path.rstrip('/')
    return f"https://staatskalender.bs.ch/organization/{new_path}"


def escape_special_chars(name: str) -> str:
    '''
    Escape special characters in asset names for Dataspot API according to the business key rules.
    
    According to Dataspot documentation, special characters need to be properly escaped in business keys:
    
    1. If a name contains / or ., it should be enclosed in double quotes
       Example: INPUT/OUTPUT → "INPUT/OUTPUT"
       Example: dataspot. → "dataspot."
    
    2. If a name contains double quotes ("), each double quote should be doubled ("") and 
       the entire name should be enclosed in double quotes
       Example: 28" City Bike → "28"" City Bike"
       Example: Project "Zeus" → "Project ""Zeus"""
    
    Args:
        name (str): The name of the asset (dataset, organizational unit, etc.)
        
    Returns:
        str: The escaped name suitable for use in Dataspot API business keys
    '''
    if not name:
        return name
    
    # Check if the name contains any characters that need special handling
    needs_quoting = False
    
    # Names containing '/' or '.' need to be quoted
    if '/' in name or '.' in name:
        needs_quoting = True
    

    # Names containing double quotes need special handling
    has_quotes = '"' in name
    if has_quotes:
        needs_quoting = True
        # Double each quote in the name
        name = "".join('""' if char == '"' else char for char in name)
    
    # Enclose the name in quotes if needed
    if needs_quoting:
        return f'"{name}"'
    
    return name
