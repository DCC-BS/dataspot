import os
import logging
from dotenv import load_dotenv
from requests.auth import HTTPBasicAuth
from src.common import requests_get


class StaatskalenderAuth:
    """Handles authentication for Staatskalender API using API key and token."""

    def __init__(self):
        load_dotenv()
        self.access_key = os.getenv("HTTPS_ACCESS_KEY_STAATSKALENDER")
        
        if not self.access_key:
            raise Exception("HTTPS_ACCESS_KEY_STAATSKALENDER environment variable is not set")
        
        # Token caching
        self.token = None

    def get_token(self):
        """Get a valid token, either from cache or by requesting a new one."""
        if self.token:
            return self.token

        return self._request_new_token()

    def _request_new_token(self):
        """Request a new token using API key authentication."""
        auth_url = "https://staatskalender.bs.ch/api/authenticate"
        
        try:
            res_auth = requests_get(
                url=auth_url,
                auth=HTTPBasicAuth(self.access_key, "")
            )
            res_auth.raise_for_status()
            
            self.token = res_auth.json()["token"]
            logging.info("Successfully obtained Staatskalender authentication token")
            return self.token

        except Exception as e:
            logging.error(f"Failed to obtain Staatskalender authentication token: {str(e)}")
            raise Exception(f"Failed to obtain Staatskalender authentication token: {str(e)}")

    def get_auth(self):
        """Get HTTPBasicAuth object for authenticated requests."""
        token = self.get_token()
        return HTTPBasicAuth(token, "")


if __name__ == "__main__":
    auth = StaatskalenderAuth()
    token = auth.get_token()
    print("✅ Successfully obtained authentication token")
    
    # Test with a sample request
    print("Testing sample request to Staatskalender...")
    test_url = "https://staatskalender.bs.ch/api/agencies?page=0"
    response = requests_get(url=test_url, auth=auth.get_auth())
    response.raise_for_status()
    
    if response.status_code == 200:
        print("✅ Authentication successful - sample request completed")
    else:
        print("❌ Request failed")
