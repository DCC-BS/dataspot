import logging

from requests import HTTPError

from dataspot_auth import DataspotAuth
from src.common import requests_get, requests_delete, requests_post
import json
import os

from dataspot_dataset import Dataset


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

    def create_new_department(self, name: str) -> None:
        """
        Create a new department. If the department already exists, do nothing.

        Args:
            name (str): The name of the department.

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
            "label": name,
            "stereotype": "DEPARTEMENT"
        }

        # Check if department already exists; skip if it does.
        try:
            url_to_check = url_join(endpoint, name)
            requests_get(url_to_check, headers=headers)
            logging.info(f"Departement {name} already exists. Skip creation...")
            return
        except HTTPError as e:
            if e.response.status_code != 404:
                raise e

        # Create new department
        try:
            requests_post(endpoint, headers=headers, json=department_data)
            logging.info(f"Departement {name} created.")
            return
        except json.JSONDecodeError as e:
            raise json.JSONDecodeError(
                f"Failed to decode JSON response from {endpoint}: {str(e)}",
                e.doc,
                e.pos
            )

    def create_new_dienststelle(self, name: str, belongs_to_department: str) -> None:
        """
        Create a new "dienststelle" in dataspot under a specific department. If the "dienststelle" already exists, do nothing.

        Args:
            name (str): The name of the dienststelle.
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
            "label": name,
            "stereotype": "DA"
        }

        # Check if dienststelle already exists
        try:
            url_to_check = url_join(endpoint, name)
            requests_get(url_to_check, headers=headers)
            logging.info(f"Dienststelle {name} already exists. Skip creation...")
            return
        except HTTPError as e:
            # If the error code is 404, then everything is fine.
            if e.response.status_code != 404:
                raise e

        # Create new dienststelle
        try:
            requests_post(endpoint, headers=headers, json=dienststelle_data)
            logging.info(f"Dienststelle {name} created.")
            return
        except json.JSONDecodeError as e:
            raise json.JSONDecodeError(
                f"Failed to decode JSON response from {endpoint}: {str(e)}",
                e.doc,
                e.pos
            )

    def create_new_sammlung(self, title: str, belongs_to_dienststelle: str) -> None:
        """
        Create a new sammlung in Dataspot under a specific dienststelle. If the sammlung already exists, do nothing.

        Args:
            title (str): The title of the sammlung.
            belongs_to_dienststelle (str): The name of the parent dienststelle.

        Raises:
            HTTPError: If the dienststelle doesn't exist or other HTTP errors occur.
            json.JSONDecodeError: If the response is not valid JSON.
        """
        # Check if parent dienststelle exists
        dienststelle_path = url_join(
            self.api_type,
            self.database_name,
            'schemes',
            self.dnk_scheme_name,
            'collections',
            belongs_to_dienststelle
        )
        dienststelle_endpoint = url_join(self.base_url, dienststelle_path)
        headers = self.auth.get_headers()
        
        try:
            response = requests_get(dienststelle_endpoint, headers=headers)
            dienststelle_uuid = response.json().get('id')
            logging.debug(f"Retrieved Dienststelle UUID: {dienststelle_uuid}")
        except HTTPError as e:
            if e.response.status_code == 404:
                raise HTTPError(f"Dienststelle '{belongs_to_dienststelle}' does not seem to exist") from e
            raise e

        # Prepare endpoint for creating sammlung
        relative_path = url_join(
            self.api_type,
            self.database_name,
            'collections',
            dienststelle_uuid,
            'collections'
        )
        endpoint = url_join(self.base_url, relative_path)

        sammlung_data = {
            "_type": "Collection",
            "label": title,
            "stereotype": "BASISSAMMLUNG"
        }

        # Check if sammlung already exists
        try:
            url_to_check = url_join(endpoint, title)
            requests_get(url_to_check, headers=headers)
            logging.info(f"Sammlung '{title}' already exists. Skipping creation...")
            return
        except HTTPError as e:
            if e.response.status_code != 404:
                raise e

        # Create new sammlung
        try:
            requests_post(endpoint, headers=headers, json=sammlung_data)
            logging.info(f"Sammlung '{title}' created successfully.")
        except json.JSONDecodeError as e:
            raise json.JSONDecodeError(
                f"Failed to decode JSON response from {endpoint}: {str(e)}",
                e.doc,
                e.pos
            )

    def create_new_dataset(self, dataset: Dataset) -> None:
        """
        Create a new dataset in Dataspot under its parent collection.
        If the dataset already exists, do nothing.

        Args:
            dataset (Dataset): The dataset instance to be uploaded.

        Raises:
            ValueError: If the dataset path is invalid
            HTTPError: If API requests fail
            json.JSONDecodeError: If response parsing fails
        """
        departement, dienststelle, sammlung, subsammlung = dataset.get_departement_dienststelle_sammlung_subsammlung()
        
        # Determine the parent collection (last non-empty element in path)
        parent_path = [p for p in [departement, dienststelle, sammlung, subsammlung] if p]
        if not parent_path:
            raise ValueError(f"Invalid path for dataset {dataset.name}: path is empty")
        
        parent_collection = parent_path[-1]
        
        # Try to get parent collection
        headers = self.auth.get_headers()
        parent_path = url_join(
            self.api_type,
            self.database_name,
            'schemes',
            self.dnk_scheme_name,
            'collections',
            parent_collection
        )
        parent_endpoint = url_join(self.base_url, parent_path)
        
        try:
            response = requests_get(parent_endpoint, headers=headers)
            parent_uuid = response.json().get('id')
        except HTTPError as e:
            if e.response.status_code == 404:
                # Only create hierarchy if parent doesn't exist
                self.create_hierarchy_for_dataset(dataset)
                # Retry getting parent after creation
                response = requests_get(parent_endpoint, headers=headers)
                parent_uuid = response.json().get('id')
            else:
                raise

        # Construct dataset endpoint
        dataset_path = url_join(
            self.api_type,
            self.database_name,
            'collections',
            parent_uuid,
            'assets'
        )
        dataset_endpoint = url_join(self.base_url, dataset_path)

        # Check if dataset exists
        try:
            requests_get(url_join(dataset_endpoint, dataset.name), headers=headers)
            logging.info(f"OGD-Dataset '{dataset.name}' already exists. Skipping creation...")
            return
        except HTTPError as e:
            if e.response.status_code != 404:
                raise

        # Create dataset
        dataset_json = dataset.to_json()
        logging.debug(f"Dataset JSON Payload: {json.dumps(dataset_json, indent=2)}")
        
        try:
            requests_post(dataset_endpoint, headers=headers, json=dataset_json)
            logging.info(f"OGD-Dataset '{dataset.name}' created successfully.")
        except HTTPError as e:
            logging.error(f"Failed to create dataset: {e.response.status_code} - {e.response.text}")
            raise

    def create_hierarchy_for_dataset(self, dataset: Dataset) -> None:
        """
        Creates the complete hierarchy (department/dienststelle/sammlung/subsammlung) for a dataset if it doesn't exist.
        
        Args:
            dataset (Dataset): The dataset containing the path information
            
        Raises:
            ValueError: If the path is invalid
        """
        departement, dienststelle, sammlung, subsammlung = dataset.get_departement_dienststelle_sammlung_subsammlung()
        
        if not departement:
            raise ValueError("Department is required")
            
        # Create department if it doesn't exist
        self.create_new_department(departement)
        
        # Create dienststelle if specified and doesn't exist
        if dienststelle:
            self.create_new_dienststelle(dienststelle, departement)
            
            # Create sammlung if specified and doesn't exist
            if sammlung:
                self.create_new_sammlung(sammlung, dienststelle)
                
                # Create subsammlung if specified and doesn't exist
                if subsammlung:
                    self.create_new_sammlung(subsammlung, sammlung)
