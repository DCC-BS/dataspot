import logging

from dotenv import load_dotenv
from requests import HTTPError

from src.dataspot_auth import DataspotAuth
from src.common import requests_get, requests_delete, requests_post
import json
import os

from src.dataspot_dataset import Dataset


def url_join(*parts: str) -> str:
    return "/".join([part.strip("/") for part in parts])

class DataspotClient:
    """Client for interacting with the Dataspot API."""

    def __init__(self):
        # TODO: Move all these infos into the env file, as they should not be hardcoded
        load_dotenv('../../.dataspot.env')

        base_url = os.getenv("DATASPOT_API_BASE_URL")
        if not base_url:
            raise ValueError("DATASPOT_API_BASE_URL environment variable is not set")

        self.base_url = base_url
        self.auth = DataspotAuth()
        self.database_name = 'test-api-renato'
        self.dnk_scheme_name = 'Datennutzungskatalog'
        self.rdm_scheme_name = 'Referenzdatenmodell'
        self.tdm_scheme_name = url_join('Technische Datenmodelle -  AUFRÄUMEN', 'collections', 'Automatisch generierte ODS-Datenmodelle')
        self._accrual_periodicity_dict = None
        self._datatype_uuid_cache = {}
    
    def download(self, relative_path, params: dict[str, str] = None, endpoint_type: str = 'rest') -> list[dict[str, str]]:
        """
        Download data from Dataspot API.
        
        Args:
            relative_path (str): The relative path for the API endpoint
            params (dict[str, str]): The query parameters that should be passed in the url, i.e. everything after the ? in the url
            endpoint_type (str): The endpoint type that should be used ('rest' or 'api').
                Use api for bulk downloads in various formats (excel, csv, json, xml, ...).
                Use rest for individual asset access in json format
        Returns:
            dict: JSON response from the API
            
        Raises:
            requests.exceptions.RequestException: If the request fails
            json.JSONDecodeError: If the response is not valid JSON
        """
        endpoint = url_join(self.base_url, endpoint_type, self.database_name, relative_path)
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
        return self.download(relative_path, params, endpoint_type='api')

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

    def teardown_dnk(self, delete_empty_collections: bool = False) -> None:
        """
        Delete all OGD datasets from the DNK scheme and optionally remove empty collections.
        
        This method:
        1. Recursively traverses all collections in the DNK scheme
        2. Deletes only datasets with stereotype "OGD"
        3. Optionally removes collections that become empty after deleting their datasets
        4. Preserves the root DNK scheme even if empty
        
        Args:
            delete_empty_collections (bool): Whether to delete empty collections after removing datasets.
                                            Defaults to False.
        
        Raises:
            requests.exceptions.RequestException: If the request fails
            json.JSONDecodeError: If the response is not valid JSON
        """
        relative_path = url_join('schemes', self.dnk_scheme_name)
        endpoint = url_join(self.base_url, 'rest', self.database_name, relative_path)
        headers = self.auth.get_headers()
        
        def delete_ogd_datasets_and_empty_collections(collection_url, is_root=False):
            """
            Recursively delete OGD datasets and optionally empty collections.
            
            Args:
                collection_url (str): URL of the collection to process
                is_root (bool): Whether this is the root DNK scheme (which should not be deleted)
                
            Returns:
                bool: True if the collection is empty after processing, False otherwise
            """
            # Get collection details
            response = requests_get(collection_url, headers=headers)
            collection_data = response.json()
            collection_uuid = collection_data.get('id')
            
            # Process datasets in this collection
            # For the root level, use 'schemes' instead of 'collections' in the URL
            if is_root:
                datasets_url = url_join(self.base_url, 'rest', self.database_name, 'schemes', collection_uuid, 'datasets')
            else:
                datasets_url = url_join(self.base_url, 'rest', self.database_name, 'collections', collection_uuid, 'datasets')
            
            datasets_response = requests_get(datasets_url, headers=headers)
            datasets_data = datasets_response.json()
            datasets = datasets_data.get('_embedded', {}).get('datasets', [])
            
            # Delete OGD datasets
            for dataset in datasets:
                dataset_url = dataset['_links']['self']['href']
                dataset_label = dataset.get('label', 'Unknown')
                
                # Check if dataset has stereotype "OGD"
                if dataset.get('stereotype') == "OGD":
                    logging.info(f"Deleting OGD dataset: {dataset_label}")
                    requests_delete(url_join(self.base_url, dataset_url), headers=headers)
            
            # Process sub-collections
            # For the root level, use 'schemes' instead of 'collections' in the URL
            if is_root:
                collections_url = url_join(self.base_url, 'rest', self.database_name, 'schemes', collection_uuid, 'collections')
            else:
                collections_url = url_join(self.base_url, 'rest', self.database_name, 'collections', collection_uuid, 'collections')
            
            collections_response = requests_get(collections_url, headers=headers)
            collections_data = collections_response.json()
            sub_collections = collections_data.get('_embedded', {}).get('collections', [])
            
            # Process each sub-collection and track which ones become empty
            empty_subcollections = []
            for sub_collection in sub_collections:
                sub_collection_url = sub_collection['_links']['self']['href']
                sub_collection_label = sub_collection.get('label', 'Unknown')
                
                # Recursively process sub-collection (not root)
                is_empty = delete_ogd_datasets_and_empty_collections(url_join(self.base_url, sub_collection_url), is_root=False)
                
                if is_empty and delete_empty_collections:
                    empty_subcollections.append((sub_collection_url, sub_collection_label))
            
            # Delete empty sub-collections if delete_empty_collections is True
            if delete_empty_collections:
                for sub_url, sub_label in empty_subcollections:
                    logging.info(f"Deleting empty collection: {sub_label}")
                    requests_delete(url_join(self.base_url, sub_url), headers=headers)
            
            # Check if this collection is now empty
            # Refresh dataset and collection data after deletions
            datasets_response = requests_get(datasets_url, headers=headers)
            datasets_data = datasets_response.json()
            remaining_datasets = datasets_data.get('_embedded', {}).get('datasets', [])
            
            collections_response = requests_get(collections_url, headers=headers)
            collections_data = collections_response.json()
            remaining_collections = collections_data.get('_embedded', {}).get('collections', [])
            
            # Return True if collection is empty (no datasets and no sub-collections)
            # But don't actually delete the root schema
            is_empty = len(remaining_datasets) == 0 and len(remaining_collections) == 0
            return is_empty and not is_root
        
        # Start the recursive deletion process from the root DNK scheme
        # Pass is_root=True to prevent deletion of the root schema
        delete_ogd_datasets_and_empty_collections(endpoint, is_root=True)
        if delete_empty_collections:
            logging.info("Finished cleaning up OGD datasets and empty collections from DNK")
        else:
            logging.info("Finished cleaning up OGD datasets from DNK")

    def download_rdm(self, language: str = "de") -> list[dict[str, str]]:
        """
        Download the Referenzdatenmodell (RDM) from Dataspot.

        Args:
            language (str): Language code for the RDM (default: "de")

        Returns:
            dict: The RDM data in JSON format
        """
        relative_path = url_join('schemes', self.rdm_scheme_name, 'download')
        params = {
            'language': language,
            'format': 'json'
        }
        return self.download(relative_path, params, endpoint_type='api')

    def rdm_accrualPeriodicity_uri_to_code(self, uri: str) -> str | None:
        # TODO: remove; probably deprecated
        """
        Convert an accrual periodicity URI to its corresponding code based on the Referenzdatenmodell (RDM).
        
        Args:
            uri (str): The URI of the accrual periodicity to convert
            
        Returns:
            str or None: The corresponding code for the given URI from the RDM as a string of length 3, e.g. '005',
                         or None if the URI is empty or None
            
        Raises:
            KeyError: If the URI is not found in the periodicity dictionary
            json.JSONDecodeError: If the response is not valid JSON
        """
        if not uri:
            return None

        # Build dict if it does not exist already
        if not self._accrual_periodicity_dict:
            relative_path = url_join('schemes',
                                     self.rdm_scheme_name,
                                     'enumerations',
                                     'Aktualisierungszyklus',
                                     'literals')

            response = self.download(relative_path)
            aktualisierungszyklus = response['_embedded']['literals']
            # Create mapping from URI to code
            self._accrual_periodicity_dict = {
                item['longText']: item['code'].zfill(3)
                for item in aktualisierungszyklus
            }

        return self._accrual_periodicity_dict[uri]

    def ods_type_to_dataspot_uuid(self, ods_type: str) -> str:
        """
        Convert an ODS data type directly to its corresponding Dataspot UUID.

        Args:
            ods_type (str): The ODS data type

        Returns:
            str: The UUID of the corresponding Dataspot type

        Raises:
            HTTPError: If the type doesn't exist or other HTTP errors occur
            json.JSONDecodeError: If the response is not valid JSON
        """
        # Map ODS types to UML data types
        type_mapping = {
            'text': '/Datentypmodell/Zeichenkette',
            'int': '/Datentypmodell/Ganzzahl',
            'boolean': '/Datentypmodell/Wahrheitswert',
            'double': '/Datentypmodell/Dezimalzahl',
            'datetime': '/Datentypmodell/Zeitpunkt',
            'date': '/Datentypmodell/Datum',
            'geo_point_2d': '/Datentypmodell/geo_point_2d',
            'geo_shape': '/Datentypmodell/geo_shape',
            'file': '/Datentypmodell/Binärdaten',
            'json_blob': '/Datentypmodell/Zeichenkette',
            'identifier': '/Datentypmodell/Identifier'
        }

        # Map the ODS type to Dataspot type path
        dataspot_type = type_mapping.get(ods_type.lower(), 'UNKNOWN TYPE')

        # Return from cache if available
        if dataspot_type in self._datatype_uuid_cache:
            return self._datatype_uuid_cache[dataspot_type]

        # Split the path to get the type name
        parts = dataspot_type.split('/')
        type_name = parts[-1]  # Get the last part of the path

        # Build the path to the Datentypmodell scheme
        endpoint = url_join(self.base_url, 'rest', self.database_name, 'schemes', 'Datentypmodell', 'datatypes', type_name)
        headers = self.auth.get_headers()

        try:
            response = requests_get(endpoint, headers=headers)
            type_uuid = response.json().get('id')

            # Cache the result
            self._datatype_uuid_cache[dataspot_type] = type_uuid
            return type_uuid

        except HTTPError as e:
            logging.error(f"Failed to get UUID for type {dataspot_type}: {str(e)}")
            raise
        except json.JSONDecodeError as e:
            raise json.JSONDecodeError(
                f"Failed to decode JSON response from {endpoint}: {str(e)}",
                e.doc,
                e.pos
            )

    def teardown_tdm(self) -> None:
        """
        Delete all collections within 'Automatisch generierte ODS-Datenmodelle' from the TDM scheme,
        but keep the parent collection and scheme intact.

        Raises:
            requests.exceptions.RequestException: If the request fails
            json.JSONDecodeError: If the response is not valid JSON
        """
        
        # First, get the UUID of the target collection
        collection_path = url_join('rest', self.database_name, 'schemes', self.tdm_scheme_name)
        endpoint = url_join(self.base_url, collection_path)
        headers = self.auth.get_headers()
        
        try:
            response = requests_get(endpoint, headers=headers)
            collection_uuid = response.json().get('id')
            logging.debug(f"Found collection UUID: {collection_uuid}")
        except HTTPError as e:
            if e.response.status_code == 404:
                logging.info(f"Collection '{self.tdm_scheme_name}' not found. Nothing to delete.")
                return
            raise e
        
        # Get all assets within the target collection
        assets_path = url_join('rest', self.database_name, 'collections', collection_uuid, 'assets')
        assets_endpoint = url_join(self.base_url, assets_path)
        
        response = requests_get(assets_endpoint, headers=headers)
        response_json = response.json()
        assets = response_json.get('_embedded', {}).get('assets', [])
        
        # Delete each asset
        for asset in assets:
            asset_url = asset['_links']['self']['href']
            asset_label = asset['label']
            logging.info(f"Deleting asset: {asset_url} - {asset_label}")
            requests_delete(url_join(self.base_url, asset_url), headers=headers)

    def tdm_create_new_asset(self, name: str, columns: list[dict] = None) -> None:
        """
        Create a new asset in the 'Automatisch generierte ODS-Datenmodelle' collection.

        Args:
            name (str): The name of the asset to create.
            columns (list[dict]): List of column information, each containing:
                - label: Human-readable label
                - name: Technical column name
                - type: Data type of the column

        Raises:
            HTTPError: If the collection doesn't exist or other HTTP errors occur
            json.JSONDecodeError: If the response is not valid JSON
        """
        # Get the UUID of the target collection
        collection_path = url_join('rest', self.database_name, 'schemes', self.tdm_scheme_name)
        endpoint = url_join(self.base_url, collection_path)
        headers = self.auth.get_headers()

        try:
            response = requests_get(endpoint, headers=headers)
            collection_uuid = response.json().get('id')
            logging.debug(f"Found collection UUID: {collection_uuid}")
        except HTTPError as e:
            if e.response.status_code == 404:
                raise HTTPError(f"Collection '{self.tdm_scheme_name}' does not exist") from e
            raise e

        # Prepare endpoint for creating the asset
        assets_path = url_join('rest', self.database_name, 'collections', collection_uuid, 'assets')
        assets_endpoint = url_join(self.base_url, assets_path)

        # Check if asset already exists
        try:
            url_to_check = url_join(assets_endpoint, name)
            requests_get(url_to_check, headers=headers)
            logging.info(f"Asset '{name}' already exists. Skip creation...")
            return
        except HTTPError as e:
            if e.response.status_code != 404:
                raise e

        # Prepare asset data
        asset_data = {
            "_type": "UmlClass",
            "label": name
        }

        # Create new asset
        try:
            response = requests_post(assets_endpoint, headers=headers, json=asset_data)
            logging.info(f"Asset '{name}' created successfully.")
            asset_href = response.json()['_links']['self']['href']
        except json.JSONDecodeError as e:
            raise json.JSONDecodeError(
                f"Failed to decode JSON response from {assets_endpoint}: {str(e)}",
                e.doc,
                e.pos
            )

        # Add all attributes
        asset_endpoint = url_join(self.base_url, asset_href, 'attributes')
        attributes = []
        if columns:
            for col in columns:
                attribute = {
                    "_type": "UmlAttribute",
                    "title": col['label'],
                    "label": col['name'],
                    "hasRange": self.ods_type_to_dataspot_uuid(col['type'])
                }
                attributes.append(attribute)

        # Append attributes to asset
        for attribute in attributes:
            try:
                requests_post(asset_endpoint, headers=headers, json=attribute)
                logging.info(f"Attribute '{attribute["label"]}' created successfully.")
            except json.JSONDecodeError as e:
                raise json.JSONDecodeError(
                    f"Failed to decode JSON response from {asset_endpoint}: {str(e)}",
                    e.doc,
                    e.pos
                )


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
        relative_path = url_join('rest', self.database_name, 'schemes', self.dnk_scheme_name, 'collections')
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
        dept_path = url_join('rest', self.database_name, 'schemes', self.dnk_scheme_name, 'collections', belongs_to_department)
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
        relative_path = url_join('rest', self.database_name, 'collections', belongs_to_department_uuid, 'collections')
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
            'rest',
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
            'rest',
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

    def create_or_update_dataset(self, dataset: Dataset) -> None:
        # TODO: Change to perform POST, PUT, and PATCH
        # possible input parameters:
        # - create_if_not_exists: bool = False
        # - update_if_exists: bool = False
        
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
            'rest',
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
            'rest',
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

    def link_DNK_bestandteile_to_TDM(self, title: str) -> None:
        """
        Links DNK dataset to TDM attributes by adding composition objects.
        For each attribute in the TDM, this creates a "Bestandteil" (composition) 
        in the DNK element.
        
        Args:
            title (str): The title/name of the dataset
            
        Raises:
            HTTPError: If API requests fail
            ValueError: If dataset or attributes are not found
            json.JSONDecodeError: If response parsing fails
        """
        logging.info(f"Linking DNK to TDM for dataset: {title}")
        headers = self.auth.get_headers()
        
        # 1. Find the DNK dataset (asset) by title
        dataset_path = self.find_dataset_path(title)
        dataset_endpoint = url_join(self.base_url, dataset_path)

        try:
            response = requests_get(dataset_endpoint, headers=headers)
            dataset_data = response.json()

            dataset_href = dataset_data['_links']['self']['href']
            dataset_id = dataset_data['id']

            if not dataset_href:
                raise ValueError(f"Dataset with title '{title}' not found in DNK")
            
            logging.debug(f"Found DNK dataset: {dataset_id}")
        except HTTPError as e:
            logging.error(f"Failed to fetch DNK scheme: {str(e)}")
            raise
        
        # 2. Find TDM attributes for this dataset
        tdm_attributes_path = self.find_tdm_attributes_path(title)
        tdm_attributes_endpoint = url_join(self.base_url, tdm_attributes_path)
        
        try:
            # Get TDM attributes of asset with matching name
            response = requests_get(tdm_attributes_endpoint, headers=headers)
            tdm_attributes_data = response.json()

            tdm_attributes = tdm_attributes_data.get('_embedded', {}).get('attributes', {})
            
            if not tdm_attributes:
                logging.warning(f"No attributes found for TDM asset '{title}'")
                return
            
            logging.debug(f"Found {len(tdm_attributes)} TDM attributes")
        except HTTPError as e:
            logging.error(f"Failed to fetch TDM asset attributes: {str(e)}")
            raise
        
        # 3. Create compositions in DNK for each TDM attribute
        compositions_endpoint = url_join(self.base_url, dataset_href, 'compositions')
        
        try:
            # Add a composition for each attribute
            for attribute in tdm_attributes:
                attribute_label = attribute.get('title')
                attribute_id = attribute.get('id')
                
                if not attribute_label:
                    logging.warning(f"Skipping attribute with missing label: {attribute}")
                    continue
                
                # Create composition object
                composition_data = {
                    "_type": "Composition",
                    "label": attribute_label,
                    "composedOf": attribute_id
                }
                
                # Check if composition already exists
                try:
                    existing_response = requests_get(
                        url_join(compositions_endpoint, attribute_label), 
                        headers=headers
                    )
                    logging.info(f"Composition for '{attribute_label}' already exists. Skipping...")
                    continue
                except HTTPError as e:
                    if e.response.status_code != 404:
                        raise
                
                # Add the composition
                requests_post(compositions_endpoint, headers=headers, json=composition_data)
                logging.info(f"Created composition for attribute '{attribute_label}'")
            
            logging.info(f"Successfully linked DNK dataset '{title}' to TDM attributes")
        except HTTPError as e:
            logging.error(f"Failed to create composition: {str(e)}")
            raise

    def find_dataset_path(self, title):
        """
        Find the path to a dataset in the DNK scheme by its title.
        
        Args:
            title (str): The title or name of the dataset
            
        Returns:
            str: The full path to the dataset
            
        Raises:
            ValueError: If the dataset is not found
        """
        
        if '/' not in title:
            # If no slash, construct the path using the standard format
            return url_join('rest', self.database_name, 'schemes', self.dnk_scheme_name, 'datasets', title)
        else:
            # If there is a slash, then we need to find the dataset by title
            datasets_path = url_join('rest', self.database_name, 'schemes', self.dnk_scheme_name, 'datasets')
            datasets_endpoint = url_join(self.base_url, datasets_path)
            response = requests_get(datasets_endpoint, headers=self.auth.get_headers())
            datasets_data = response.json()
            datasets = datasets_data.get('_embedded', {}).get('datasets', [])
            for dataset in datasets:
                if dataset.get('label') == title:
                    return dataset['_links']['self']['href']
            raise ValueError(f"Dataset with title '{title}' not found in DNK")

    def find_tdm_attributes_path(self, title):
        """
        Determine the path to TDM attributes for a dataset based on its title.
        
        Args:
            title (str): The title or name of the dataset
            
        Returns:
            str: The full path to the TDM attributes
            
        Raises:
            ValueError: If the TDM asset is not found
        """
        if '/' not in title:
            # If no slash, construct the path using the standard format
            return url_join('rest', self.database_name, 'schemes', self.tdm_scheme_name, 'classifiers', title, 'attributes')
        else:
            # If there is a slash, we need to find the TDM asset by title
            assets_path = url_join('rest', self.database_name, 'schemes', self.tdm_scheme_name, 'assets')
            assets_endpoint = url_join(self.base_url, assets_path)
            response = requests_get(assets_endpoint, headers=self.auth.get_headers())
            assets_data = response.json()
            assets = assets_data.get('_embedded', {}).get('assets', [])
            
            for asset in assets:
                if asset.get('label') == title:
                    return url_join(asset['_links']['self']['href'], 'attributes')
                    
            raise ValueError(f"TDM asset with title '{title}' not found")

