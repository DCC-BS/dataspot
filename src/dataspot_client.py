import logging

from requests import HTTPError

from dataspot_auth import DataspotAuth
from src.common import requests_get, requests_delete, requests_post
import json
import os

def url_join(*parts: str) -> str:
    return "/".join([part.strip("/") for part in parts])

class DataspotClient:
    """Client for interacting with the Dataspot API."""
    
    def __init__(self, base_url):
        self.base_url = base_url
        self.auth = DataspotAuth()
        self.api_type = 'rest'
        self.database_name = 'test-api-renato'
        self.dnk_scheme_name = 'Datennutzungskatalog'
    
    def download(self, relative_path, params: dict[str, str] = None) -> list[dict[str, str]]:
        """
        Download data from Dataspot API.
        
        Args:
            relative_path (str): The relative path for the API endpoint
            params (dict[str, str]): The query parameters that should be passed in the url, i.e. everything after the ? in the url
            
        Returns:
            dict: JSON response from the API
            
        Raises:
            requests.exceptions.RequestException: If the request fails
            json.JSONDecodeError: If the response is not valid JSON
        """
        endpoint = url_join(self.base_url, 'api', self.database_name, relative_path)
        headers = self.auth.get_headers()
        
        response = requests_get(endpoint, headers=headers, params=params)
        
        try:
            return response.json()
        except json.JSONDecodeError as e:
            raise json.JSONDecodeError(
                f"Failed to decode JSON response from {endpoint}: {str(e)}", 
                e.doc, 
                e.pos
            )

    def download_dnk(self, language: str = "de") -> list[dict[str, str]]:
        """
        Download the Datennutzungskatalog (DNK) from Dataspot.
        
        Args:
            language (str): Language code for the DNK (default: "de")
            
        Returns:
            dict: The DNK data in JSON format
        """
        relative_path = url_join('schemes', self.dnk_scheme_name, 'download')
        params = {
            'language': language,
            'format': 'json'
        }
        return self.download(relative_path, params)
    
    def save_dnk(self, output_dir: str = "tmp", language: str = "de") -> str:
        """
        Download and save the DNK to a file.
        
        Args:
            output_dir (str): Directory to save the file (default: "tmp")
            language (str): Language code for the DNK (default: "de")
            
        Returns:
            str: Path to the saved file
        """
        os.makedirs(output_dir, exist_ok=True)
        
        dnk_data = self.download_dnk(language)
        
        output_path = os.path.join(output_dir, "Datennutzungskatalog.json")
        with open(output_path, 'w', encoding='utf-8') as f:
            json.dump(dnk_data, f, ensure_ascii=False, indent=2)
            
        return output_path

    def teardown_dnk(self) -> None:
        """
        Delete all collections from the DNK scheme, but keep the empty DNK.
        
        Raises:
            requests.exceptions.RequestException: If the request fails
        """
        relative_path = url_join(self.api_type, self.database_name, 'schemes', self.dnk_scheme_name, 'collections')
        endpoint = url_join(self.base_url, relative_path)
        headers = self.auth.get_headers()
        
        # Get all collections
        response = requests_get(endpoint, headers=headers)
        response_json = response.json()
        response_json_collections = response_json.get('_embedded', {}).get('collections', {})
        collections = [item['_links']['self']['href'] for item in response_json_collections]

        # Delete each collection
        for collection_relative_path in collections:
            collection_url = url_join(self.base_url, collection_relative_path)
            logging.info(f"Deleting collection: {collection_url}")
            requests_delete(collection_url, headers=headers)

    def create_new_department(self, title: str) -> None:
        """
        Create a new department. If the department already exists, do nothing.

        Args:
            title (str): The title of the department.

        Returns:
            dict: The created department metadata as returned by the API.

        Raises:
            json.JSONDecodeError: If the response is not valid JSON.
        """
        relative_path = url_join(self.api_type, self.database_name, 'schemes', self.dnk_scheme_name, 'collections')
        endpoint = url_join(self.base_url, relative_path)
        headers = self.auth.get_headers()

        department_data = {
            "_type": "Collection",
            "label": title,
            "stereotype": "DEPARTEMENT"
        }

        # Check if department already exists; skip if it does.
        try:
            url_to_check = url_join(endpoint, title)
            requests_get(url_to_check, headers=headers)
            logging.info(f"Departement {title} already exists. Skip creation...")
            return
        except HTTPError as e:
            if e.response.status_code != 404:
                raise e

        # Create new department
        try:
            requests_post(endpoint, headers=headers, json=department_data)
            logging.info(f"Departement {title} created.")
            return
        except json.JSONDecodeError as e:
            raise json.JSONDecodeError(
                f"Failed to decode JSON response from {endpoint}: {str(e)}",
                e.doc,
                e.pos
            )

    def create_new_dienststelle(self, title: str, belongs_to_department: str) -> None:
        """
        Create a new "dienststelle" in dataspot under a specific department. If the "dienststelle" already exists, do nothing.

        Args:
            title (str): The title of the dienststelle.
            belongs_to_department (str): The title of the parent department.

        Raises:
            HTTPError: If the department doesn't exist or other HTTP errors occur.
            json.JSONDecodeError: If the response is not valid JSON.
        """
        # Check if parent department exists
        dept_path = url_join(self.api_type, self.database_name, 'schemes', self.dnk_scheme_name, 'collections', belongs_to_department)
        dept_endpoint = url_join(self.base_url, dept_path)
        headers = self.auth.get_headers()
        
        try:
            r = requests_get(dept_endpoint, headers=headers)
            belongs_to_department_uuid = r.json().get('id')
            logging.debug(f"Retrieved Department UUID: {belongs_to_department_uuid}")
        except HTTPError as e:
            if e.response.status_code == 404:
                raise HTTPError(f"Department '{belongs_to_department}' does not seem to exist") from e
            raise e

        # Prepare endpoint for creating dienststelle
        relative_path = url_join(self.api_type, self.database_name, 'collections', belongs_to_department_uuid, 'collections')
        endpoint = url_join(self.base_url, relative_path)

        dienststelle_data = {
            "_type": "Collection",
            "label": title,
            "stereotype": "DA"
        }

        # Check if dienststelle already exists
        try:
            url_to_check = url_join(endpoint, title)
            requests_get(url_to_check, headers=headers)
            logging.info(f"Dienststelle {title} already exists. Skip creation...")
            return
        except HTTPError as e:
            # If the error code is 404, then everything is fine.
            if e.response.status_code != 404:
                raise e

        # Create new dienststelle
        try:
            requests_post(endpoint, headers=headers, json=dienststelle_data)
            logging.info(f"Dienststelle {title} created.")
            return
        except json.JSONDecodeError as e:
            raise json.JSONDecodeError(
                f"Failed to decode JSON response from {endpoint}: {str(e)}",
                e.doc,
                e.pos
            )
