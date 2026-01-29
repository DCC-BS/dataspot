"""
KDM API client: authentication and a single interface from application code to the KDM API.

Uses OAuth 2.0 client_credentials to obtain a Bearer token. KDM API GET requests bypass
the system proxy (proxies=None) so traffic goes directly to the API host; this avoids
503 from the proxy when the browser works by not using the proxy for that host.
"""

import logging
import os
from datetime import datetime, timedelta

import requests
from dotenv import load_dotenv

from src.common import requests_get


# ---------------------------------------------------------------------------
# KDM OAuth auth
# ---------------------------------------------------------------------------

class KdmAuth:
    """Handles OAuth 2.0 client_credentials authentication for the KDM API."""

    def __init__(self):
        load_dotenv()
        self.token_url = os.getenv("KDM_TOKEN_URL")
        self.client_id = os.getenv("KDM_CLIENT_ID")
        self.client_secret = os.getenv("KDM_CLIENT_SECRET")
        self.scope = os.getenv("KDM_SCOPE", "")
        self.token = None
        self.token_expires_at = None
        self._validate_config()

    def _validate_config(self):
        if not self.token_url or not self.client_id or not self.client_secret:
            raise ValueError(
                "KDM OAuth config missing. Set KDM_TOKEN_URL, KDM_CLIENT_ID, KDM_CLIENT_SECRET (and optionally KDM_SCOPE) in env or .env"
            )

    def get_bearer_access_token(self) -> str:
        """Return a valid token, from cache or by requesting a new one."""
        if self._is_token_valid():
            return self.token
        return self._request_new_bearer_token()

    def _is_token_valid(self) -> bool:
        if not self.token or not self.token_expires_at:
            return False
        return datetime.now() < self.token_expires_at - timedelta(minutes=5)

    def _request_new_bearer_token(self) -> str:
        data = {
            "client_id": self.client_id,
            "client_secret": self.client_secret,
            "grant_type": "client_credentials",
        }
        if self.scope:
            data["scope"] = self.scope

        response = requests.post(self.token_url, data=data)
        response.raise_for_status()
        token_data = response.json()
        self.token = token_data["access_token"]
        expires_in = int(token_data.get("expires_in", 3600))
        self.token_expires_at = datetime.now() + timedelta(seconds=expires_in)
        logging.info("Obtained new KDM OAuth Bearer token")
        return self.token

    def get_headers(self) -> dict:
        """Headers for KDM API requests: Authorization Bearer and Accept."""
        return {
            "Authorization": f"Bearer {self.get_bearer_access_token()}",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        }


# ---------------------------------------------------------------------------
# KDM API client interface
# ---------------------------------------------------------------------------

class KdmClient:
    """Single interface to the KDM API: auth + HTTP methods. Pass full URLs to each method."""

    def __init__(self):
        self.auth = KdmAuth()

    def get(self, url: str, **kwargs) -> requests.Response:
        """GET url (full URL). Bypasses proxy for KDM. kwargs passed to requests_get (e.g. timeout, params)."""
        headers = kwargs.pop("headers", None) or self.auth.get_headers()
        kwargs.setdefault("proxies", {"http": None, "https": None})
        return requests_get(url=url, headers=headers, **kwargs)

# ---------------------------------------------------------------------------
# Convenience: default client and test
# ---------------------------------------------------------------------------

def get_kdm_client() -> KdmClient:
    """Return a KdmClient."""
    return KdmClient()


if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)
    client = get_kdm_client()
    response = client.get(os.getenv("KDM_TEST_URL_TMP"))
    response.raise_for_status()
    logging.info("KDM API request succeeded: %s", response.status_code)
