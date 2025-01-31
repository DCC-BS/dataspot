import os
import urllib3
import ssl
import requests

from dotenv import load_dotenv
from src.common.retry import *

http_errors_to_handle = (
    ConnectionResetError,
    urllib3.exceptions.MaxRetryError,
    requests.exceptions.ProxyError,
    requests.exceptions.HTTPError,
    ssl.SSLCertVerificationError,
)

load_dotenv(os.path.join(os.path.dirname(__file__), '../../..', '.env'))
proxies = {
    'http': os.getenv('HTTP_PROXY'),
    'https': os.getenv('HTTPS_PROXY')
}

@retry(http_errors_to_handle, tries=6, delay=5, backoff=1)
def requests_get(*args, **kwargs):
    r = requests.get(*args, proxies=proxies, **kwargs)
    r.raise_for_status()
    return r


@retry(http_errors_to_handle, tries=6, delay=5, backoff=1)
def requests_post(*args, **kwargs):
    r = requests.post(*args, proxies=proxies, **kwargs)
    r.raise_for_status()
    return r


@retry(http_errors_to_handle, tries=6, delay=5, backoff=1)
def requests_patch(*args, **kwargs):
    r = requests.patch(*args, proxies=proxies, **kwargs)
    r.raise_for_status()
    return r


@retry(http_errors_to_handle, tries=6, delay=5, backoff=1)
def requests_put(*args, **kwargs):
    r = requests.put(*args, proxies=proxies, **kwargs)
    r.raise_for_status()
    return r


@retry(http_errors_to_handle, tries=6, delay=5, backoff=1)
def requests_delete(*args, **kwargs):
    r = requests.delete(*args, proxies=proxies, **kwargs)
    r.raise_for_status()
    return r