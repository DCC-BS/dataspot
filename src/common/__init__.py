"""
Dataspot API Request Handlers

This module provides wrapper functions around the requests library for making HTTP requests to the Dataspot API.
It includes:

- Automatic retry logic for various HTTP/network errors
- Rate limiting to prevent server overload (this is the only module that handles rate limiting)
- Proxy support via environment variables
- Error message parsing and logging
- Support for all common HTTP methods (GET, POST, PUT, PATCH, DELETE)

The default rate limit delay between requests is 1 second but can be customized per request.
"""

import json
import os
from json import JSONDecodeError

import urllib3
import ssl
import requests

from dotenv import load_dotenv
from urllib3.exceptions import HTTPError

from src.common.retry import *



# Default rate limit to avoid overloading the server
RATE_LIMIT_DELAY_SEC = 1.0

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

load_dotenv(os.path.join(os.path.dirname(__file__), '../../..', '.proxy.env'))
proxies = {
    'http': os.getenv('HTTP_PROXY'),
    'https': os.getenv('HTTPS_PROXY')
}

def _print_potential_error_messages(response: requests.Response) -> None:
    try:
        if response.status_code not in [200, 201, 204]: # 200 is normal, 201 is ???, 204 is normal return code for delete
            error_message_detailed = json.loads(response.content.decode(response.apparent_encoding))
            logging.error(f"{error_message_detailed['method']} unsuccessful: {error_message_detailed['message']}")
            violations = error_message_detailed.get('violations', [])
            if violations:
                logging.error(f"Found {len(violations)} violations:")
                for violation in violations:
                    logging.error(violation)

    except JSONDecodeError or HTTPError:
        exit(f"Error {response.status_code}: Cannot perform {response.request.method} because  '{response.reason}' for url {response.url}")

@retry(http_errors_to_handle, tries=1, delay=5, backoff=1)
def requests_get(*args, **kwargs):
    # Extract delay parameter or use default
    delay = kwargs.pop('rate_limit_delay', RATE_LIMIT_DELAY_SEC)
    
    r = requests.get(*args, proxies=proxies, **kwargs)

    _print_potential_error_messages(r)
    r.raise_for_status()
    
    # Add delay after request to avoid overloading the server
    time.sleep(delay)
    return r


@retry(http_errors_to_handle, tries=2, delay=5, backoff=1)
def requests_post(*args, **kwargs):
    # Extract delay parameter or use default
    delay = kwargs.pop('rate_limit_delay', RATE_LIMIT_DELAY_SEC)
    
    r = requests.post(*args, proxies=proxies, **kwargs)
    _print_potential_error_messages(r)
    r.raise_for_status()
    
    # Add delay after request to avoid overloading the server
    time.sleep(delay)
    return r


@retry(http_errors_to_handle, tries=2, delay=5, backoff=1)
def requests_patch(*args, **kwargs):
    # Extract delay parameter or use default
    delay = kwargs.pop('rate_limit_delay', RATE_LIMIT_DELAY_SEC)
    
    r = requests.patch(*args, proxies=proxies, **kwargs)
    _print_potential_error_messages(r)
    r.raise_for_status()
    
    # Add delay after request to avoid overloading the server
    time.sleep(delay)
    return r


@retry(http_errors_to_handle, tries=2, delay=5, backoff=1)
def requests_put(*args, **kwargs):
    # Extract delay parameter or use default
    delay = kwargs.pop('rate_limit_delay', RATE_LIMIT_DELAY_SEC)
    
    r = requests.put(*args, proxies=proxies, **kwargs)
    _print_potential_error_messages(r)
    r.raise_for_status()
    
    # Add delay after request to avoid overloading the server
    time.sleep(delay)
    return r


@retry(http_errors_to_handle, tries=2, delay=5, backoff=1)
def requests_delete(*args, **kwargs):
    # Extract delay parameter or use default
    delay = kwargs.pop('rate_limit_delay', RATE_LIMIT_DELAY_SEC)
    
    r = requests.delete(*args, proxies=proxies, **kwargs)
    _print_potential_error_messages(r)
    r.raise_for_status()
    
    # Add delay after request to avoid overloading the server
    time.sleep(delay)
    return r