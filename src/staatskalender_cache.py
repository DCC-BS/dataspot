import os
import logging
from typing import Dict, Optional

from dotenv import load_dotenv
from requests.auth import HTTPBasicAuth
from src.common import requests_get

class StaatskalenderCache:
    """
    Centralized cache for Staatskalender API data (memberships and persons).
    
    This class provides cached access to Staatskalender data, avoiding redundant
    API calls. All API errors propagate after retries are exhausted (fail-fast behavior).
    """
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

    def __init__(self):
        """Initialize the cache with empty caches and authentication."""
        self._membership_cache: Dict[str, Dict] = {}
        self._person_cache: Dict[str, Dict] = {}
        self._auth = self.StaatskalenderAuth()
    
    def get_membership(self, membership_id: str) -> Dict:
        """
        Get membership data by membership ID (cached).
        
        Args:
            membership_id: The Staatskalender membership ID
            
        Returns:
            dict: Membership data with keys:
                - 'membership_id': str
                - 'person_id': str (extracted from person link)
                - 'person_link': str (full href)
                
        Raises:
            DetailedHTTPError: If API request fails after retries
            Exception: If person link cannot be found in membership data
        """
        # Check cache first
        if membership_id in self._membership_cache:
            logging.debug(f"Using cached membership data for {membership_id}")
            return self._membership_cache[membership_id]
        
        logging.debug(f"Retrieving membership data from Staatskalender for membership ID: {membership_id}")
        
        # Retrieve membership data from staatskalender
        membership_url = f"https://staatskalender.bs.ch/api/memberships/{membership_id}"
        membership_response = requests_get(url=membership_url, auth=self._auth.get_auth())
        
        # Extract person link from membership data
        membership_data = membership_response.json()
        person_link = None
        
        for item in membership_data.get('collection', {}).get('items', []):
            for link in item.get('links', []):
                if link.get('rel') == 'person':
                    person_link = link.get('href')
                    break
            if person_link:
                break
        
        if not person_link:
            raise Exception(f"Could not find person link in membership data for membership ID {membership_id}")
        
        # Extract person_id from person_link (last part of URL)
        person_id = person_link.rsplit('/', 1)[1]
        
        # Cache and return membership data
        membership_info = {
            'membership_id': membership_id,
            'person_id': person_id,
            'person_link': person_link
        }
        
        self._membership_cache[membership_id] = membership_info
        logging.debug(f"Cached membership data for {membership_id}")
        
        return membership_info
    
    def get_person_by_id(self, person_id: str) -> Dict:
        """
        Get person data by person ID (cached).
        
        Args:
            person_id: The Staatskalender person ID
            
        Returns:
            dict: Person data with keys:
                - 'person_id': str
                - 'given_name': str
                - 'additional_name': Optional[str]
                - 'family_name': str
                - 'email': Optional[str]
                - 'phone': Optional[str]
                
        Raises:
            DetailedHTTPError: If API request fails after retries
        """
        # Check cache first
        if person_id in self._person_cache:
            logging.debug(f"Using cached person data for {person_id}")
            return self._person_cache[person_id]
        
        logging.debug(f"Retrieving person data from Staatskalender for person ID: {person_id}")
        
        # Get person data from Staatskalender
        person_url = f"https://staatskalender.bs.ch/api/people/{person_id}"
        person_response = requests_get(url=person_url, auth=self._auth.get_auth())
        
        # Extract person details
        person_data = person_response.json()
        sk_email = None
        sk_phone = None
        sk_first_name = None
        sk_additional_name = None
        sk_last_name = None
        
        for item in person_data.get('collection', {}).get('items', []):
            for data_item in item.get('data', []):
                field_name = data_item.get('name')
                field_value = data_item.get('value')
                
                if field_name == 'email':
                    sk_email = field_value
                elif field_name == 'phone' or field_name == 'telephone' or field_name == 'phone_number':
                    sk_phone = field_value
                elif field_name == 'first_name':
                    # Split first_name into givenName and additionalName
                    raw_first_name = field_value
                    if raw_first_name:
                        cleaned_first_name = raw_first_name.strip()
                        if cleaned_first_name:
                            parts = cleaned_first_name.split(' ', 1)
                            sk_first_name = parts[0]
                            sk_additional_name = parts[1] if len(parts) > 1 else None
                elif field_name == 'last_name':
                    sk_last_name = field_value
                    if sk_last_name:
                        sk_last_name = sk_last_name.strip() if sk_last_name.strip() else None
        
        # Cache and return person data
        person_info = {
            'person_id': person_id,
            'given_name': sk_first_name,
            'additional_name': sk_additional_name,
            'family_name': sk_last_name,
            'email': sk_email,
            'phone': sk_phone
        }
        
        self._person_cache[person_id] = person_info
        logging.debug(f"Cached person data for {person_id}")
        
        return person_info
    
    def get_person_by_membership(self, membership_id: str) -> Dict:
        """
        Get person data via membership ID (cached).
        
        This method first retrieves the membership to get the person link,
        then retrieves the person data.
        
        Args:
            membership_id: The Staatskalender membership ID
            
        Returns:
            dict: Person data (same format as get_person_by_id)
            
        Raises:
            DetailedHTTPError: If API request fails after retries
            Exception: If membership or person data cannot be retrieved
        """
        # Get membership to find person_id
        membership_info = self.get_membership(membership_id)
        person_id = membership_info['person_id']
        
        # Get person data
        return self.get_person_by_id(person_id)
    
    def get_person_email(self, person_id: str) -> Optional[str]:
        """
        Get email address for a person (cached).
        
        Args:
            person_id: The Staatskalender person ID
            
        Returns:
            Optional[str]: Email address if available, None otherwise
            
        Raises:
            DetailedHTTPError: If API request fails after retries
        """
        person_data = self.get_person_by_id(person_id)
        return person_data.get('email')
    
    def get_person_contact_details(self, person_id: str) -> Dict:
        """
        Get full contact details for a person (cached).
        
        Args:
            person_id: The Staatskalender person ID
            
        Returns:
            dict: Contact details with keys:
                - 'email': Optional[str]
                - 'phone': Optional[str]
                
        Raises:
            DetailedHTTPError: If API request fails after retries
        """
        person_data = self.get_person_by_id(person_id)
        return {
            'email': person_data.get('email'),
            'phone': person_data.get('phone')
        }


if __name__ == "__main__":
    auth = StaatskalenderCache.StaatskalenderAuth()
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
