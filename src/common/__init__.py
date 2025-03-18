import json
import os
import urllib3
import ssl
import requests
import time

from dotenv import load_dotenv
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

@retry(http_errors_to_handle, tries=1, delay=5, backoff=1)
def requests_get(*args, **kwargs):
    # Extract delay parameter or use default
    delay = kwargs.pop('rate_limit_delay', RATE_LIMIT_DELAY_SEC)
    
    r = requests.get(*args, proxies=proxies, **kwargs)
    if r.status_code == 400:
        error_message_detailed = json.loads(r.content.decode(r.apparent_encoding))
        logging.error(f"{error_message_detailed['method']} unsuccessful: {error_message_detailed['message']}")
    r.raise_for_status()
    
    # Add delay after request to avoid overloading the server
    time.sleep(delay)
    return r


@retry(http_errors_to_handle, tries=2, delay=5, backoff=1)
def requests_post(*args, **kwargs):
    # Extract delay parameter or use default
    delay = kwargs.pop('rate_limit_delay', RATE_LIMIT_DELAY_SEC)
    
    r = requests.post(*args, proxies=proxies, **kwargs)
    if r.status_code == 400:
        error_message_detailed = json.loads(r.content.decode(r.apparent_encoding))
        logging.error(f"{error_message_detailed['method']} unsuccessful: {error_message_detailed['message']}")
    r.raise_for_status()
    
    # Add delay after request to avoid overloading the server
    time.sleep(delay)
    return r


@retry(http_errors_to_handle, tries=2, delay=5, backoff=1)
def requests_patch(*args, **kwargs):
    # Extract delay parameter or use default
    delay = kwargs.pop('rate_limit_delay', RATE_LIMIT_DELAY_SEC)
    
    r = requests.patch(*args, proxies=proxies, **kwargs)
    if r.status_code == 400:
        error_message_detailed = json.loads(r.content.decode(r.apparent_encoding))
        logging.error(f"{error_message_detailed['method']} unsuccessful: {error_message_detailed['message']}")
    r.raise_for_status()
    
    # Add delay after request to avoid overloading the server
    time.sleep(delay)
    return r


@retry(http_errors_to_handle, tries=2, delay=5, backoff=1)
def requests_put(*args, **kwargs):
    # Extract delay parameter or use default
    delay = kwargs.pop('rate_limit_delay', RATE_LIMIT_DELAY_SEC)
    
    r = requests.put(*args, proxies=proxies, **kwargs)
    if r.status_code == 400:
        error_message_detailed = json.loads(r.content.decode(r.apparent_encoding))
        logging.error(f"{error_message_detailed['method']} unsuccessful: {error_message_detailed['message']}")
    r.raise_for_status()
    
    # Add delay after request to avoid overloading the server
    time.sleep(delay)
    return r


@retry(http_errors_to_handle, tries=2, delay=5, backoff=1)
def requests_delete(*args, **kwargs):
    # Extract delay parameter or use default
    delay = kwargs.pop('rate_limit_delay', RATE_LIMIT_DELAY_SEC)
    
    r = requests.delete(*args, proxies=proxies, **kwargs)
    if r.status_code == 400:
        error_message_detailed = json.loads(r.content.decode(r.apparent_encoding))
        logging.error(f"{error_message_detailed['method']} unsuccessful: {error_message_detailed['message']}")
    r.raise_for_status()
    
    # Add delay after request to avoid overloading the server
    time.sleep(delay)
    return r