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

def get_uuid_and_href_from_response(response: dict) -> tuple:
    """
    Extract both UUID and href from a Dataspot API response.
    
    The UUID is at response['id']. The href is at response['_links']['self']['href'].
    
    Args:
        response (dict): The JSON response from Dataspot API
        
    Returns:
        tuple: (uuid, href) where both are strings or None if not found
        
    Example:
        >>> uuid, href = get_uuid_and_href_from_response(response)
        >>> if uuid and href:
        >>>     # Use the UUID and href
    """
    uuid = response.get('id')
    href = response.get('_links', {}).get('self', {}).get('href')
    return uuid, href

def generate_potential_staatskalender_url(path: str) -> str:
    """
    Generate a URL for the Basel Staatskalender based on an organization path string.
    
    This function transforms an organization path into a standardized URL format
    by applying these transformations:
    - Convert to lowercase
    - Replace spaces with hyphens
    - Convert German umlauts (ö→oe, ä→ae, ü→ue)
    - Remove all characters except letters, hyphens, and forward slashes
    - Replace double hyphens with single ones
    - Remove trailing slashes
    
    Args:
        path (str): The organization path string to transform
        
    Returns:
        str: A formatted URL pointing to the organization in the Basel Staatskalender
        
    Example:
        >>> generate_potential_staatskalender_url("Präsidialdepartement/Kantons- und Stadtentwicklung")
        "https://staatskalender.bs.ch/organization/praesidialdepartement/kantons-und-stadtentwicklung"
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
    
    if name is None:
        logging.warning(f"Trying to escape special characters for None")

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
