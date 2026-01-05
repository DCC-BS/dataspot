import logging
import re
from typing import Optional, List, Dict, Any

import requests


# Module-level storage for tracking invalid URLs during extraction
invalid_urls: List[Dict[str, Any]] = []

# Module-level cache for extracted descriptions (maps URL -> description or None)
_description_cache: Dict[str, Optional[str]] = {}


def clear_description_cache() -> None:
    """Clear the description cache."""
    global _description_cache
    _description_cache = {}


def build_org_descriptions_cache(org_data: Dict[str, Any]) -> Dict[str, Optional[str]]:
    """
    Build a cache of descriptions for all unique website URLs in the organization data.
    This allows batch processing before transformation, making the process more efficient.
    
    Args:
        org_data: Organization data from ODS API containing 'results' list
        
    Returns:
        Dict mapping normalized URLs to description strings (or None if not found).
        Invalid URLs (those that cause errors) are not included in this cache.
    """
    cache: Dict[str, Optional[str]] = {}
    
    if not org_data or 'results' not in org_data:
        return cache
    
    # Collect all unique website URLs with their org titles and staatskalender urls
    url_to_orgs: Dict[str, List[str]] = {}  # url -> list of org titles
    url_to_staatskalender_urls: Dict[str, List[str]] = {}  # url -> url to org website in staatskalender
    
    for org in org_data['results']:
        website = org.get('website', '')
        if website:
            url_normalized = _normalize_url(website)
            org_title = org.get('title', '').strip()
            org_staatskalender_url = org.get('url_website')

            assert org_title
            assert len(org_title) > 0
            assert org_staatskalender_url

            if url_normalized not in url_to_orgs:
                url_to_orgs[url_normalized] = []
                url_to_staatskalender_urls[url_normalized] = []
            if org_title:
                url_to_orgs[url_normalized].append(org_title)
            if org_staatskalender_url:
                url_to_staatskalender_urls[url_normalized].append(org_staatskalender_url)

    # Extract descriptions for all unique URLs
    unique_urls = list(url_to_orgs.keys())
    if unique_urls:
        logging.info(f"Pre-extracting descriptions for {len(unique_urls)} unique website URLs...")
        for url_normalized in unique_urls:
            # Use the first org title for error reporting
            org_titles = url_to_orgs[url_normalized] if url_to_orgs[url_normalized] else None
            org_staatskalender_urls = url_to_staatskalender_urls[url_normalized]
            
            # Track invalid URLs count before calling retrieve_meta_description
            invalid_count_before = len(invalid_urls)
            
            # Try to retrieve description
            description = retrieve_meta_description(url_normalized, org_titles=org_titles, org_staatskalender_urls=org_staatskalender_urls)
            
            # Only add to cache if no error occurred (invalid_urls count didn't increase)
            invalid_count_after = len(invalid_urls)
            if invalid_count_after == invalid_count_before:
                # No error occurred, add to cache (even if description is None)
                cache[url_normalized] = description
        
        logging.info(f"Completed pre-extraction of descriptions ({len(cache)} URLs cached, {len(unique_urls) - len(cache)} invalid URLs excluded)")
    
    return cache


def retrieve_meta_description(url: str, org_titles: [str], org_staatskalender_urls: [str]) -> Optional[str]:
    """
    Extract the meta description from a webpage.
    
    Args:
        url: The URL of the webpage to extract the description from.
        org_titles: Titles of the organization (for error reporting).
        org_staatskalender_urls: The URL of the Staatskalender entry for the organization (for error reporting).

    Returns:
        The meta description content if found, None otherwise.
        
    Note:
        - Only processes URLs containing '.bs.ch'
        - Skips URLs that are exactly 'www.bs.ch' (with or without protocol/trailing slash)
        - Returns None on any error (network, parsing, missing tag)
        - Network errors are tracked in invalid_urls module-level variable
        - Results are cached to avoid fetching the same URL multiple times
    """
    if not url:
        return None
    
    url_normalized = _normalize_url(url)
    
    # Check cache first
    if url_normalized in _description_cache:
        cached_description = _description_cache[url_normalized]
        if cached_description is not None:
            logging.info(f"Using cached description for {url_normalized}")
        return cached_description
    
    # Validate URL contains '.bs.ch'
    if '.bs.ch' not in url_normalized:
        logging.info(f"Skipping URL (not a bs.ch address): {url}")
        _description_cache[url_normalized] = None
        return None
    
    # Check if URL is exactly 'www.bs.ch' (skip these)
    if url_normalized == 'www.bs.ch':
        logging.info(f"Skipping URL (bare www.bs.ch without path): {url}")
        _description_cache[url_normalized] = None
        return None
    
    try:
        # Fetch the webpage - always use http:// prefix for requests
        fetch_url = 'http://' + url_normalized
        response = requests.get(fetch_url)
        response.raise_for_status()
        html_content = response.text
        
        # Extract meta description using regex
        # Pattern matches: <meta name="description" content="...">
        pattern = r'<meta\s+name="description"\s+content="([^"]*)"'
        match = re.search(pattern, html_content, re.IGNORECASE)
        
        if match:
            description = match.group(1).strip()
            if description:
                logging.info(f"Extracted description from {url_normalized}")
                _description_cache[url_normalized] = description
                return description
            else:
                logging.info(f"Meta description tag found but empty: {url_normalized}")
                _description_cache[url_normalized] = None
                return None
        else:
            logging.info(f"No meta description tag found: {url_normalized}")
            _description_cache[url_normalized] = None
            return None
            
    except requests.RequestException as e:
        error_msg = str(e)
        logging.warning(f"Network error fetching {url_normalized}: {error_msg}")
        for (org_staatskalender_url, org_title) in zip(org_staatskalender_urls, org_titles):
            logging.warning(f" - {org_staatskalender_url}: {org_title}")
        # Track this as an invalid URL (use original url for reporting)
        invalid_urls.append({
            'url': url,
            'org_titles': org_titles,
            'org_staatskalender_urls': org_staatskalender_urls,
            'error': error_msg
        })
        _description_cache[url_normalized] = None
        return None
    except Exception as e:
        error_msg = str(e)
        logging.warning(f"Error while processing {url_normalized}: {error_msg}")
        # Track this as an invalid URL (use original url for reporting)
        invalid_urls.append({
            'url': url,
            'org_titles': org_titles,
            'org_staatskalender_urls': org_staatskalender_urls,
            'error': error_msg
        })
        _description_cache[url_normalized] = None
        return None


def _normalize_url(url: str) -> Optional[str]:
    """
    Normalize the URL by stripping the protocol, and ensuring 'www.' and trailing slash exists
    """
    if not url:
        return None
    normalized_url = url.removeprefix('https://').removeprefix('http://').removesuffix('/')
    if not normalized_url.startswith('www.'):
        normalized_url = 'www.' + normalized_url
    logging.debug(f"Normalized URL: {url} -> {normalized_url}")
    return normalized_url
