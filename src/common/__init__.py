"""
Dataspot API Request Handlers

This module provides wrapper functions around the requests library for making HTTP requests to the Dataspot API.
It includes:

- Automatic retry logic for various HTTP/network errors
- Rate limiting to prevent server overload (this is the only module that handles rate limiting)
- Proxy support via environment variables
- Detailed error message parsing and logging
- Support for all common HTTP methods (GET, POST, PUT, PATCH, DELETE)
- Custom DetailedHTTPError exception that preserves detailed error information from API responses

The default rate limit delay between requests is 1 second but can be customized per request.

When errors occur, the functions raise DetailedHTTPError exceptions that contain the full error details
from the API response, including violations and specific error messages, rather than generic HTTP error messages.
"""

import json
import logging
import os
import time
from json import JSONDecodeError

import urllib3
import ssl
import requests

from urllib3.exceptions import HTTPError

from src.common.retry import *


# Default rate limit to avoid overloading the server
RATE_LIMIT_DELAY_SEC = 1.0


class DetailedHTTPError(requests.exceptions.HTTPError):
    """Custom HTTPError that includes detailed error information from the response."""
    
    def __init__(self, response, detailed_error_info=None):
        self.response = response
        self.detailed_error_info = detailed_error_info or {}
        
        # Create a detailed error message
        if detailed_error_info and 'message' in detailed_error_info:
            error_msg = detailed_error_info['message']
            
            # Add violations to the error message
            violations = detailed_error_info.get('violations', [])
            if violations:
                violation_messages = [v.get('message', str(v)) for v in violations]
                error_msg += f" Violations: {'; '.join(violation_messages)}"
            
            # Add other errors to the error message
            errors = detailed_error_info.get('errors', [])
            if errors:
                error_messages = [str(e) for e in errors]
                error_msg += f" Errors: {'; '.join(error_messages)}"
                
        else:
            error_msg = f"{response.status_code} Client Error: {response.reason} for url: {response.url}"
        
        super().__init__(error_msg)
    
    def get_detailed_error_info(self):
        """Return the detailed error information dict."""
        return self.detailed_error_info

http_errors_to_handle = (
    ConnectionResetError,
    urllib3.exceptions.MaxRetryError,
    requests.exceptions.ProxyError,
    requests.exceptions.HTTPError,
    ssl.SSLCertVerificationError,
    requests.ConnectionError,
    requests.ConnectTimeout,
    requests.ReadTimeout,
    requests.Timeout,
)

def _get_detailed_error_info(response: requests.Response, silent_status_codes: list = None) -> dict:
    """
    Parse and return detailed error information from a response.
    
    Args:
        response: The HTTP response object
        silent_status_codes: A list of status codes that should not trigger error logging
        
    Returns:
        dict: Detailed error information including message, violations, and errors
    """
    # Initialize silent_status_codes if not provided
    if silent_status_codes is None:
        silent_status_codes = []
        
    # Skip processing for status codes that should be handled silently
    if response.status_code in [200, 201, 204] or response.status_code in silent_status_codes:
        return {}
        
    try:
        error_message_detailed = json.loads(response.content.decode(response.apparent_encoding))
        
        # Log the error information
        try:
            logging.error(f"{error_message_detailed['method']} unsuccessful: {error_message_detailed['message']}")
        except KeyError:
            logging.error(f"Call unsuccessful: {error_message_detailed['message']}")

        violations = error_message_detailed.get('violations', [])
        if violations:
            logging.error(f"Found {len(violations)} violations:")
            for violation in violations:
                logging.error(violation)

        errors = error_message_detailed.get('errors', [])
        if errors:
            logging.error(f"Found {len(errors)} errors:")
            for error in errors:
                logging.error(error)
        
        return error_message_detailed

    except (JSONDecodeError, HTTPError):
        # If we can't parse the JSON, return a basic error structure
        basic_error = {
            'message': f"Cannot perform {response.request.method} because '{response.reason}' for url {response.url}",
            'status_code': response.status_code,
            'reason': response.reason,
            'url': response.url
        }
        logging.error(f"Error {response.status_code}: {basic_error['message']}")
        return basic_error

@retry(http_errors_to_handle, tries=1, delay=5, backoff=1)
def requests_get(*args, **kwargs):
    # Extract parameters
    delay = kwargs.pop('rate_limit_delay', RATE_LIMIT_DELAY_SEC)
    silent_status_codes = kwargs.pop('silent_status_codes', None)
    
    r = requests.get(*args, **kwargs)

    # Get detailed error information
    detailed_error_info = _get_detailed_error_info(r, silent_status_codes)
    
    # If there's an error, raise a custom exception with detailed info
    if r.status_code not in [200, 201, 204] and r.status_code not in (silent_status_codes or []):
        raise DetailedHTTPError(r, detailed_error_info)
    
    # Add delay after request to avoid overloading the server
    time.sleep(delay)
    return r


@retry(http_errors_to_handle, tries=2, delay=5, backoff=1)
def requests_post(*args, **kwargs):
    # Extract parameters
    delay = kwargs.pop('rate_limit_delay', RATE_LIMIT_DELAY_SEC)
    silent_status_codes = kwargs.pop('silent_status_codes', None)
    
    r = requests.post(*args, **kwargs)
    
    # Get detailed error information
    detailed_error_info = _get_detailed_error_info(r, silent_status_codes)
    
    # If there's an error, raise a custom exception with detailed info
    if r.status_code not in [200, 201, 204] and r.status_code not in (silent_status_codes or []):
        raise DetailedHTTPError(r, detailed_error_info)
    
    # Add delay after request to avoid overloading the server
    time.sleep(delay)
    return r


@retry(http_errors_to_handle, tries=2, delay=5, backoff=1)
def requests_patch(*args, **kwargs):
    # Extract parameters
    delay = kwargs.pop('rate_limit_delay', RATE_LIMIT_DELAY_SEC)
    silent_status_codes = kwargs.pop('silent_status_codes', None)
    
    r = requests.patch(*args, **kwargs)
    
    # Get detailed error information
    detailed_error_info = _get_detailed_error_info(r, silent_status_codes)
    
    # If there's an error, raise a custom exception with detailed info
    if r.status_code not in [200, 201, 204] and r.status_code not in (silent_status_codes or []):
        raise DetailedHTTPError(r, detailed_error_info)
    
    # Add delay after request to avoid overloading the server
    time.sleep(delay)
    return r


@retry(http_errors_to_handle, tries=2, delay=5, backoff=1)
def requests_put(*args, **kwargs):
    # Extract parameters
    delay = kwargs.pop('rate_limit_delay', RATE_LIMIT_DELAY_SEC)
    silent_status_codes = kwargs.pop('silent_status_codes', None)
    
    r = requests.put(*args, **kwargs)
    
    # Get detailed error information
    detailed_error_info = _get_detailed_error_info(r, silent_status_codes)
    
    # If there's an error, raise a custom exception with detailed info
    if r.status_code not in [200, 201, 204] and r.status_code not in (silent_status_codes or []):
        raise DetailedHTTPError(r, detailed_error_info)
    
    # Add delay after request to avoid overloading the server
    time.sleep(delay)
    return r


@retry(http_errors_to_handle, tries=2, delay=5, backoff=1)
def requests_delete(*args, **kwargs):
    # Extract parameters
    delay = kwargs.pop('rate_limit_delay', RATE_LIMIT_DELAY_SEC)
    silent_status_codes = kwargs.pop('silent_status_codes', None)
    
    r = requests.delete(*args, **kwargs)
    
    # Get detailed error information
    detailed_error_info = _get_detailed_error_info(r, silent_status_codes)
    
    # If there's an error, raise a custom exception with detailed info
    if r.status_code not in [200, 201, 204] and r.status_code not in (silent_status_codes or []):
        raise DetailedHTTPError(r, detailed_error_info)
    
    # Add delay after request to avoid overloading the server
    time.sleep(delay)
    return r