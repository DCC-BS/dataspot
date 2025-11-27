import logging
from typing import List, Dict, Any

from src.clients.base_client import BaseDataspotClient
from src.clients.helpers import url_join, get_uuid_from_response
from src.dataspot_dataset import Dataset
from src.mapping_handlers.base_dataspot_handler import BaseDataspotHandler
from src.mapping_handlers.base_dataspot_mapping import BaseDataspotMapping


class DatasetMapping(BaseDataspotMapping):
    """
    A lookup table that maps ODS IDs to Dataspot asset type, UUID, and optionally inCollection.
    Stores the mapping in a CSV file for persistence. Handles only datasets for now.
    The REST endpoint is constructed dynamically.
    """

    def __init__(self, database_name: str, scheme: str):
        """
        Initialize the mapping table for datasets.
        The CSV filename is derived from the database_name and scheme.

        Args:
            database_name (str): Name of the database to use for file naming.
                                 Example: "feature-staatskalender_DNK_ods-dataspot-mapping.csv"
            scheme (str): Name of the scheme (e.g., 'DNK', 'TDM')
        """
        super().__init__(database_name, "odsDataportalId", "ods-dataspot", scheme)


class DatasetHandler(BaseDataspotHandler):
    """
    Handler for dataset synchronization operations in Dataspot.
    Provides methods to sync datasets between ODS and Dataspot.
    """
    # Set configuration values for the base handler
    asset_id_field = 'odsDataportalId'
    
    def __init__(self, client: BaseDataspotClient):
        """
        Initialize the DatasetHandler.
        
        Args:
            client: BaseDataspotClient instance to use for API operations
        """
        # Call parent's __init__ method first
        super().__init__(client)
        
        # Initialize the dataset mapping
        self.mapping = DatasetMapping(database_name=client.database_name, scheme=client.scheme_name_short)

        # Set the asset type filter based on asset_id_field
        self.asset_type_filter = lambda asset: asset.get(self.asset_id_field) is not None

        # Check for special characters in the default path and name
        if any('/' in folder for folder in self.client.ods_imports_collection_path) \
            or any('.' in folder for folder in self.client.ods_imports_collection_path) \
            or ('/' in self.client.ods_imports_collection_name) \
            or '.' in self.client.ods_imports_collection_name:
            # TODO: Implement escaping of special characters in the default dataset path
            logging.error("The default path or name in config.py contains special characters ('/' or '.') that need escaping. This functionality is not yet supported and needs to be properly implemented as needed.")
            raise ValueError("The default path or name in config.py contains special characters ('/' or '.') that need escaping. This functionality is not yet supported and needs to be properly implemented as needed.")

        if self.client.ods_imports_collection_path:
            self.default_dataset_path_full = url_join(*self.client.ods_imports_collection_path, self.client.ods_imports_collection_name)
        else:
            self.default_dataset_path_full = self.client.ods_imports_collection_name

        logging.debug(f"Default dataset path: {self.default_dataset_path_full}")

    def sync_datasets(self, datasets: List[Dataset], status: str = "WORKING") -> Dict[str, Any]:
        """
        Synchronize datasets between ODS and Dataspot.
        This is the main public method for dataset synchronization.
        
        The method:
        1. Updates mappings before upload
        2. Separates datasets into new and existing based on odsDataportalId
        3. Updates existing datasets individually to preserve other fields
        4. Creates new datasets using bulk upload
        5. Updates mappings after all operations
        6. Saves mappings to CSV

        Args:
            datasets: List of Dataset objects to synchronize with Dataspot
            status: Status to set on created/updated datasets. Defaults to "WORKING" (DRAFT group).
                   Use "PUBLISHED" to make datasets public immediately.
            
        Returns:
            Dict[str, Any]: Report of the synchronization process
        """
        if not datasets:
            logging.warning("No datasets provided for synchronization")
            return {
                "status": "error",
                "message": "No datasets provided for synchronization",
                "datasets_processed": 0
            }

        logging.info(f"Starting synchronization of {len(datasets)} datasets...")

        # Step 1: Update mappings before upload
        logging.info("Step 1: Updating mappings before upload...")
        self.update_mappings_before_upload()

        # Step 2: Extract ODS IDs and separate datasets into new and existing
        logging.info("Step 2: Separating datasets into new and existing...")
        new_datasets = []
        existing_datasets = []
        odsDataportalIds = []
        
        for dataset in datasets:
            dataset_json = dataset.to_json()
            odsDataportalId = dataset_json.get('customProperties', {}).get('odsDataportalId')
            if odsDataportalId:
                odsDataportalIds.append(odsDataportalId)
                
                # Check if dataset exists in mapping
                existing_entry = self.mapping.get_entry(odsDataportalId)
                if existing_entry:
                    existing_datasets.append((dataset, odsDataportalId, existing_entry))
                else:
                    new_datasets.append(dataset)

        # Prepare results structure with detailed change tracking
        result = {
            "status": "success",
            "datasets_processed": len(datasets),
            "created": 0,
            "updated": 0,
            "unchanged": 0,
            "errors": 0,
            "deleted": 0,
            "details": {
                "creations": {"count": 0, "items": []},
                "updates": {"count": 0, "items": []},
                "deletions": {"count": 0, "items": []},
                "errors": {"count": 0, "items": []}
            }
        }

        # Step 3: Process existing datasets with individual updates to preserve other fields
        logging.info(f"Step 3: Updating {len(existing_datasets)} existing datasets individually...")
        
        if existing_datasets:
            for dataset, odsDataportalId, entry in existing_datasets:
                try:
                    # entry format is (_type, uuid, inCollection)
                    uuid = entry[1]
                    
                    # Get the endpoint for this dataset
                    endpoint = f"/rest/{self.client.database_name}/datasets/{uuid}"
                    
                    # First, get the current dataset to compare
                    current_dataset = self.client._get_asset(endpoint=endpoint)
                    if not current_dataset:
                        # Dataset no longer exists in Dataspot
                        logging.warning(f"Dataset with odsDataportalId {odsDataportalId} no longer exists in Dataspot")
                        result["errors"] += 1
                        result["details"]["errors"]["count"] += 1
                        result["details"]["errors"]["items"].append({
                            "odsDataportalId": odsDataportalId,
                            "message": "Dataset no longer exists in Dataspot"
                        })
                        continue
                    
                    # New dataset data
                    dataset_json = dataset.to_json()
                    
                    # Compare fields to track changes
                    changes = {}
                    
                    # Fields to compare (add more as needed)
                    compare_fields = [
                        ("label", "Title"),
                        ("description", "Description"),
                        ("shortDescription", "Short Description")
                    ]
                    
                    # Compare basic fields
                    for field_name, display_name in compare_fields:
                        old_value = current_dataset.get(field_name)
                        new_value = dataset_json.get(field_name)
                        
                        if old_value != new_value:
                            changes[display_name] = {
                                "old_value": old_value,
                                "new_value": new_value
                            }
                    
                    # Compare custom properties
                    current_custom_props = current_dataset.get('customProperties', {})
                    new_custom_props = dataset_json.get('customProperties', {})
                    
                    for prop_key in set(list(current_custom_props.keys()) + list(new_custom_props.keys())):
                        old_value = current_custom_props.get(prop_key)
                        new_value = new_custom_props.get(prop_key)
                        
                        if old_value != new_value:
                            changes[f"Custom Property: {prop_key}"] = {
                                "old_value": old_value,
                                "new_value": new_value
                            }
                    
                    # Only update if there are actual changes
                    if changes:
                        # Ensure inCollection is preserved from the mapping
                        dataset_json['inCollection'] = current_dataset['inCollection']

                        # Use the client's update method with replace=False to do a PATCH
                        try:
                            # Update the existing dataset
                            response = self.client._update_asset(
                                endpoint=endpoint, 
                                data=dataset_json, 
                                replace=False,
                                status=status
                            )
                            
                            # Extract UUID (should be the same as entry[1])
                            uuid = entry[1]
                            
                            # Track successful update
                            result["updated"] += 1
                            result["details"]["updates"]["count"] += 1
                            
                            # Create update details entry
                            title = dataset_json.get('label', f"<Unnamed Dataset {odsDataportalId}>")
                            ods_url = f"https://data.bs.ch/explore/dataset/{odsDataportalId}"
                            
                            update_entry = {
                                "odsDataportalId": odsDataportalId,
                                "title": title,
                                "link": ods_url,
                                "uuid": uuid,  # Add the UUID to the update entry
                                "changes": changes
                            }
                            
                            result["details"]["updates"]["items"].append(update_entry)
                            logging.info(f"Successfully updated dataset with odsDataportalId {odsDataportalId}: {title}")
                            
                            # Log detailed changes
                            for field, values in changes.items():
                                logging.debug(f"  - Changed {field}:")
                                logging.debug(f"    - Old: {values.get('old_value')}")
                                logging.debug(f"    - New: {values.get('new_value')}")
                                
                        except Exception as e:
                            error_msg = f"Error updating dataset with odsDataportalId {odsDataportalId}: {str(e)}"
                            logging.error(error_msg)
                            
                            result["errors"] += 1
                            result["details"]["errors"]["count"] += 1
                            result["details"]["errors"]["items"].append({
                                "odsDataportalId": odsDataportalId,
                                "message": error_msg
                            })
                    else:
                        # No changes needed
                        result["unchanged"] += 1
                        logging.info(f"No changes needed for dataset with odsDataportalId {odsDataportalId}")
                
                except Exception as e:
                    error_msg = f"Error processing update for dataset with odsDataportalId {odsDataportalId}: {str(e)}"
                    logging.error(error_msg)
                    
                    result["errors"] += 1
                    result["details"]["errors"]["count"] += 1
                    result["details"]["errors"]["items"].append({
                        "odsDataportalId": odsDataportalId,
                        "message": error_msg
                    })

        # Step 4: Upload new datasets using bulk_create_or_update_datasets
        logging.info(f"Step 4: Creating {len(new_datasets)} new datasets with bulk upload...")
        
        if new_datasets:
            try:
                # Process new datasets
                for dataset in new_datasets:
                    dataset_json = dataset.to_json()
                    odsDataportalId = dataset_json.get('customProperties', {}).get('odsDataportalId')
                    title = dataset_json.get('label', f"<Unnamed Dataset {odsDataportalId}>")
                    
                    try:
                        # Create the dataset
                        response = self.create_dataset(dataset, status=status)
                        
                        # Get UUID from response
                        uuid = get_uuid_from_response(response)
                        
                        # Track successful creation
                        result["created"] += 1
                        result["details"]["creations"]["count"] += 1
                        
                        # Create creation details entry
                        ods_url = f"https://data.bs.ch/explore/dataset/{odsDataportalId}"
                        
                        creation_entry = {
                            "odsDataportalId": odsDataportalId,
                            "title": title,
                            "link": ods_url,
                            "uuid": uuid
                        }
                        
                        result["details"]["creations"]["items"].append(creation_entry)
                        logging.info(f"Successfully created dataset with odsDataportalId {odsDataportalId}: {title}")
                        
                    except Exception as e:
                        error_msg = f"Error creating dataset with odsDataportalId {odsDataportalId}: {str(e)}"
                        logging.error(error_msg)
                        
                        result["errors"] += 1
                        result["details"]["errors"]["count"] += 1
                        result["details"]["errors"]["items"].append({
                            "odsDataportalId": odsDataportalId,
                            "message": error_msg
                        })
                
            except Exception as e:
                error_msg = f"Error during bulk creation: {str(e)}"
                logging.error(error_msg)
                result["errors"] += 1

        # Step 5: Update mappings after all operations
        logging.info("Step 5: Updating mappings after upload...")
        if odsDataportalIds:
            self.update_mappings_after_upload(odsDataportalIds, result)

        # Step 6: Save mappings to CSV
        logging.info("Step 6: Saving mappings to CSV...")
        self.mapping.save_to_csv()

        # Generate result message
        result["message"] = (
            f"Synchronized {len(datasets)} datasets: "
            f"{result['created']} created, {result['updated']} updated, "
            f"{result['unchanged']} unchanged, {result['deleted']} deleted, "
            f"{result['errors']} errors"
        )

        logging.info(f"Dataset synchronization completed: {result['updated']} updated, {result['created']} created, {result['deleted']} deleted, {result['errors']} errors")
        return result

    def update_mappings_after_upload(self, odsDataportalIds: List[str], result: Dict[str, Any] = None) -> None:
        """
        Updates the mapping between ODS IDs and Dataspot UUIDs after uploading datasets.
        Uses the download API to retrieve all datasets and then updates the mapping for matching ODS IDs.
        Also updates the creation/update items in the result with UUIDs if provided.
        
        Args:
            odsDataportalIds (List[str]): List of ODS IDs to update in the mapping
            result (Dict[str, Any], optional): Result dictionary to update with UUIDs
            
        Raises:
            HTTPError: If API requests fail
            ValueError: If unable to retrieve dataset information
        """
        # Call the base class method with our specific ID type
        super().update_mappings_after_upload(odsDataportalIds)
        
        # Update sync result with UUIDs if result is provided
        if result and 'details' in result:
            details = result['details']
            
            # Update UUIDs in creation items
            if 'creations' in details and details['creations']['count'] > 0:
                for i, creation in enumerate(details['creations']['items']):
                    odsDataportalId = creation.get('odsDataportalId')
                    if odsDataportalId:
                        # Get the UUID from mapping
                        entry = self.mapping.get_entry(odsDataportalId)
                        if entry and len(entry) >= 2:
                            uuid = entry[1]
                            details['creations']['items'][i]['uuid'] = uuid
            
            # Update UUIDs in update items
            if 'updates' in details and details['updates']['count'] > 0:
                for i, update in enumerate(details['updates']['items']):
                    odsDataportalId = update.get('odsDataportalId')
                    if odsDataportalId and not update.get('uuid'):  # Only update if not already set
                        # Get the UUID from mapping
                        entry = self.mapping.get_entry(odsDataportalId)
                        if entry and len(entry) >= 2:
                            uuid = entry[1]
                            details['updates']['items'][i]['uuid'] = uuid

    def bulk_create_or_update_datasets(self, datasets: List[Dataset],
                                      operation: str = "ADD", dry_run: bool = False) -> dict:
        """
        Bulk create or update datasets in the DNK scheme using the bulk upload API.
        The datasets will be created at the scheme level, but each dataset will have its inCollection
        field set to place it within the ODS-Imports collection or the collection stored in the mapping.
        
        Args:
            datasets (List[Dataset]): The dataset instances to be uploaded
            operation (str): The operation to perform. Can be "ADD" (add/update), "REPLACE" (replace) or "FULL_LOAD" (completely replace)
            dry_run (bool): Whether to do a dry run (not actually modify the data)
            
        Returns:
            dict: The JSON response from the API containing the upload results
            
        Raises:
            ValueError: If any dataset is missing required properties
            HTTPError: If API requests fail
        """
        # Verify we have datasets to process
        if not datasets:
            logging.warning("No datasets provided for bulk upload")
            return {"status": "error", "message": "No datasets provided"}
        
        # Get the ODS-Imports collection UUID
        collection_uuid = self.client._ods_imports_collection.get('id')

        if not collection_uuid:
            error_msg = "Failed to get collection UUID"
            logging.error(error_msg)
            raise ValueError(error_msg)

        logging.debug(f"Using collection UUID: {collection_uuid}")
        
        # Validate and transform datasets
        dataset_jsons = []
        odsDataportalIds = []
        validation_errors = []
        
        for i, dataset in enumerate(datasets):
            # Get ODS ID from dataset
            dataset_json = dataset.to_json()
            odsDataportalId = dataset_json.get('customProperties', {}).get('odsDataportalId')
            
            if not odsDataportalId:
                validation_errors.append(f"Dataset at index {i} missing 'odsDataportalId' property")
                continue
            
            odsDataportalIds.append(odsDataportalId)
            
            # Read the dataset title for logging
            title = dataset_json.get('label', f"<Unnamed Dataset {odsDataportalId}>")
            logging.debug(f"Processing dataset '{title}' with ODS ID: {odsDataportalId}")
            
            # Check if this dataset has a stored inCollection (business key)
            inCollection = self.mapping.get_inCollection(odsDataportalId)
            
            if inCollection:
                # Use the stored inCollection business key
                logging.debug(f"Using stored inCollection '{inCollection}' for dataset with ODS ID: {odsDataportalId}")
                dataset_json['inCollection'] = inCollection
            else:
                # Use the centralized default dataset path
                logging.debug(f"Using default dataset path: '{self.default_dataset_path_full}' for dataset with ODS ID: {odsDataportalId}")
                dataset_json['inCollection'] = self.default_dataset_path_full
            
            dataset_jsons.append(dataset_json)
        
        # If we encountered validation errors, raise an exception
        if validation_errors:
            error_msg = f"Validation errors in {len(validation_errors)} datasets: {validation_errors[:3]}"
            if len(validation_errors) > 3:
                error_msg += f" and {len(validation_errors) - 3} more"
            logging.error(error_msg)
            raise ValueError(error_msg)
            
        # Count of datasets
        num_datasets = len(dataset_jsons)
        if num_datasets == 0:
            logging.warning("No valid datasets to upload after validation")
            return {"status": "error", "message": "No valid datasets to upload"}
            
        logging.info(f"Bulk creating {num_datasets} datasets (operation: {operation}, dry_run: {dry_run})...")
        
        # Bulk create datasets using the scheme name
        response = self.client.bulk_create_or_update_assets(
            scheme_name=self.scheme_name,
            data=dataset_jsons,
            operation=operation,
            dry_run=dry_run
        )

        logging.info(f"Bulk creation complete")

        # Update mapping for each dataset (only for non-dry runs)
        if not dry_run:
            # After bulk upload, retrieve the datasets and update mapping
            self.update_mappings_after_upload(odsDataportalIds, response)
        
        logging.info(f"Bulk dataset creation completed successfully")
        return response

    def create_dataset(self, dataset: Dataset, status: str = "WORKING") -> dict:
        """
        Create a new dataset in the 'Datennutzungskatalog/ODS-Imports' in Dataspot.
        
        Args:
            dataset (Dataset): The dataset instance to be uploaded.
            status: Status to set on the dataset. Defaults to "WORKING" (DRAFT group).
                   Use "PUBLISHED" to make the dataset public immediately.
            
        Returns:
            dict: The JSON response from the API containing the dataset data
            
        Raises:
            ValueError: If the dataset is missing required properties
            HTTPError: If API requests fail
            json.JSONDecodeError: If response parsing fails
        """
        # Get ODS ID from dataset
        odsDataportalId = dataset.to_json().get('customProperties', {}).get('odsDataportalId')
        if not odsDataportalId:
            logging.error("Dataset missing 'odsDataportalId' property required for ODS ID")
            raise ValueError("Dataset must have an 'odsDataportalId' property to use as ODS ID")
        
        # Check if dataset with this ODS ID already exists
        existing_entry = self.mapping.get_entry(odsDataportalId)
        if existing_entry:
            # Entry is now (_type, uuid, inCollection)
            _type, uuid, _ = existing_entry
            logging.info(f"Dataset with ODS ID {odsDataportalId} already exists (Type: {_type}, UUID: {uuid}). Use update_dataset or create_or_update_dataset method to update.")
            raise ValueError(f"Dataset with ODS ID {odsDataportalId} already exists. Use update_dataset or create_or_update_dataset method.")
        
        # Read the dataset title
        title = dataset.to_json()['label']
        logging.info(f"Creating dataset: '{title}' with ODS ID: {odsDataportalId}")

        # Get the collection UUID
        collection_uuid = self.client._ods_imports_collection.get('id')

        if not collection_uuid:
            error_msg = "Failed to get collection UUID"
            logging.error(error_msg)
            raise ValueError(error_msg)

        collection_href = url_join('rest', self.database_name, 'collections', collection_uuid, leading_slash=True)
        logging.debug(f"Using collection UUID: {collection_uuid} and constructed href: {collection_href}")
        
        # Create a new dataset
        dataset_creation_endpoint = url_join(collection_href, "datasets")
        
        # Ensure inCollection property is set with the full path
        dataset_json = dataset.to_json()

        response = self.client._create_asset(
            endpoint=dataset_creation_endpoint,
            data=dataset_json,
            status=status
        )
        
        # Store the mapping for future reference
        if odsDataportalId:
            uuid = get_uuid_from_response(response)
            if uuid:
                # For newly created datasets, store the ODS-Imports collection name as the business key
                # The _type for datasets created here is always "Dataset"
                logging.debug(f"Adding mapping entry for ODS ID {odsDataportalId} with Type 'Dataset', UUID {uuid}, and inCollection '{self.client.ods_imports_collection_name}'")
                self.mapping.add_entry(odsDataportalId, "Dataset", uuid, self.client.ods_imports_collection_name)
            else:
                logging.warning(f"Could not extract UUID from response for dataset '{title}'")
        
        logging.info(f"Successfully created dataset '{title}'")
        return response

    def update_dataset(self, dataset: Dataset, href: str, force_replace: bool = False) -> dict:
        """
        Update an existing dataset in the DNK.
        
        Args:
            dataset (Dataset): The dataset instance with updated data
            href (str): The href of the dataset to update
            force_replace (bool): Whether to completely replace the dataset (True) or just update properties (False)
            
        Returns:
            dict: The JSON response from the API containing the updated dataset data
            
        Raises:
            ValueError: If the dataset is missing required properties
            HTTPError: If API requests fail
            json.JSONDecodeError: If response parsing fails
        """
        # Get ODS ID from dataset
        odsDataportalId = dataset.to_json().get('customProperties', {}).get('odsDataportalId')
        if not odsDataportalId:
            logging.error("Dataset missing 'odsDataportalId' property required for ODS ID")
            raise ValueError("Dataset must have an 'odsDataportalId' property to use as ODS ID")
        
        # Read the dataset title
        title = dataset.to_json()['label']
        logging.info(f"Updating dataset: '{title}' with ODS ID: {odsDataportalId}")
        
        # Get the inCollection from mapping if available (this is now a business key)
        inCollection = self.mapping.get_inCollection(odsDataportalId)
        
        # Set inCollection in the dataset JSON
        dataset_json = dataset.to_json()
        if inCollection:
            dataset_json['inCollection'] = inCollection
            logging.debug(f"Using stored inCollection '{inCollection}' from mapping")
        else:
            # Default to ODS-Imports collection with full path
            collection_path = self.client.ods_imports_collection_path
            
            if collection_path:
                # Use the centralized default dataset path
                logging.debug(f"Using default dataset path: '{self.default_dataset_path_full}'")
                dataset_json['inCollection'] = self.default_dataset_path_full
            else:
                # No path, just use the collection name directly
                logging.debug(f"Using default inCollection: '{self.client.ods_imports_collection_name}'")
                dataset_json['inCollection'] = self.client.ods_imports_collection_name
        
        # Update the existing dataset
        logging.debug(f"Update method: {'PUT (replace)' if force_replace else 'PATCH (partial update)'}")
        response = self.client._update_asset(
            endpoint=href,
            data=dataset_json,
            replace=force_replace
        )
        
        # Ensure the mapping is updated
        if odsDataportalId:
            uuid = get_uuid_from_response(response)
            if uuid:
                # The _type for datasets updated here is always "Dataset"
                # Use the determined inCollection value (either from mapping or default)
                final_inCollection = dataset_json.get('inCollection')
                logging.debug(f"Updating mapping for ODS ID {odsDataportalId} with Type 'Dataset', UUID {uuid}, inCollection {final_inCollection}")
                self.mapping.add_entry(odsDataportalId, "Dataset", uuid, final_inCollection)
            else:
                logging.warning(f"Could not extract UUID from response for dataset '{title}'")
        
        logging.info(f"Successfully updated dataset '{title}'")
        return response

    def create_or_update_dataset(self, dataset: Dataset, update_strategy: str = 'create_or_update',
                                 force_replace: bool = False) -> dict:
        """
        Create a new dataset or update an existing dataset in the 'Datennutzungskatalog/ODS-Imports' in Dataspot.
        All datasets are placed directly in the ODS-Imports collection, regardless of their internal path structure.

        The method behavior is controlled by the update_strategy parameter:
        - 'create_only': Only creates a new dataset using POST. Fails if the dataset already exists.
        - 'update_only': Only updates an existing dataset. Fails if the dataset doesn't exist.
        - 'create_or_update' (default): Creates a new dataset if it doesn't exist, updates it if it does.

        The force_replace parameter controls the update behavior:
        - False (default): Uses PATCH to update only the specified properties, preserving other properties.
        - True: Uses PUT to completely replace the dataset with the new values.

        Args:
            dataset (Dataset): The dataset instance to be uploaded.
            update_strategy (str): Strategy for handling dataset existence ('create_only', 'update_only', 'create_or_update').
            force_replace (bool): Whether to completely replace an existing dataset (True) or just update properties (False).

        Returns:
            dict: The JSON response from the API containing the dataset data

        Raises:
            ValueError: If the update_strategy is invalid
            HTTPError: If API requests fail
            json.JSONDecodeError: If response parsing fails
        """
        # Validate update strategy
        valid_strategies = ['create_only', 'update_only', 'create_or_update']
        if update_strategy not in valid_strategies:
            logging.error(f"Invalid update_strategy: {update_strategy}. Must be one of {valid_strategies}")
            raise ValueError(f"Invalid update_strategy: {update_strategy}. Must be one of {valid_strategies}")
        
        # Get ODS ID from dataset
        odsDataportalId = dataset.to_json().get('customProperties', {}).get('odsDataportalId')
        if not odsDataportalId:
            logging.error("Dataset missing 'odsDataportalId' property required for ODS ID")
            raise ValueError("Dataset must have an 'odsDataportalId' property to use as ODS ID")
        
        # Read the dataset title
        title = dataset.to_json()['label']
        logging.info(f"Processing dataset: '{title}' with ODS ID: {odsDataportalId}")
        
        # Check if dataset exists in Dataspot
        dataset_exists = False
        href = None
        
        # Check mapping for existing entry
        logging.debug(f"Checking if dataset with ODS ID {odsDataportalId} exists in mapping")
        entry = self.mapping.get_entry(odsDataportalId)
        if entry:
            dataset_exists = True
            # Build the API href from the UUID (which is the second item in the entry tuple)
            uuid = entry[1]
            href = url_join('rest', self.database_name, 'datasets', uuid, leading_slash=True)
            logging.debug(f"Found existing dataset in mapping with UUID: {uuid}, building href: {href}")
            
            # Verify that the dataset still exists at this href
            logging.debug(f"Verifying dataset still exists at: {href}")
            asset_data = self.client._get_asset(href)
            if not asset_data:
                # Dataset doesn't exist at the expected location
                logging.warning(f"Dataset no longer exists at {href}, removing from mapping")
                dataset_exists = False
                self.mapping.remove_entry(odsDataportalId)
        
        # Handle according to update strategy
        if dataset_exists:
            if update_strategy == 'create_only':
                logging.error(f"Dataset '{title}' already exists and update_strategy is 'create_only'")
                raise ValueError(f"Dataset '{title}' already exists and update_strategy is 'create_only'")
            
            if update_strategy in ['update_only', 'create_or_update']:
                # Update the existing dataset
                return self.update_dataset(dataset, href, force_replace)
        else:
            if update_strategy == 'update_only':
                logging.error(f"Dataset '{title}' does not exist and update_strategy is 'update_only'")
                raise ValueError(f"Dataset '{title}' does not exist and update_strategy is 'update_only'")
            
            if update_strategy in ['create_only', 'create_or_update']:
                # Create a new dataset
                return self.create_dataset(dataset)
        
        # This should not happen if the code is correct
        logging.error("Unexpected error in create_or_update_dataset")
        raise RuntimeError("Unexpected error in create_or_update_dataset")

    def delete_dataset(self, odsDataportalId: str, fail_if_not_exists: bool = False) -> bool:
        """
        Delete a dataset from the DNK or mark it for deletion review.
        
        Args:
            odsDataportalId (str): The ODS ID of the dataset to delete
            fail_if_not_exists (bool): Whether to raise an error if the dataset doesn't exist
            
        Returns:
            bool: True if the dataset was deleted or marked for deletion, or if it didn't exist but tracking was updated.
                 False if it didn't exist in the mapping and fail_if_not_exists is False.
            
        Raises:
            ValueError: If the dataset doesn't exist and fail_if_not_exists is True
            HTTPError: If API requests fail
        """
        # Check if the dataset exists in the mapping
        entry = self.mapping.get_entry(odsDataportalId)
        
        if not entry:
            if fail_if_not_exists:
                raise ValueError(f"Dataset with ODS ID '{odsDataportalId}' does not exist in mapping")
            logging.warning(f"Dataset with ODS ID '{odsDataportalId}' not found in mapping, cannot delete.")
            return False
        
        # Get UUID from the entry and build the href dynamically
        _type, uuid, _inCollection = entry
        href = url_join('rest', self.database_name, 'datasets', uuid, leading_slash=True)
        
        # Check if the dataset still exists in Dataspot
        asset_exists = self.client._get_asset(href) is not None
        
        if asset_exists:
            # Dataset exists, mark it for deletion
            logging.info(f"Marking dataset with ODS ID '{odsDataportalId}' (UUID: {uuid}) for deletion review at {href}")
            self.client._mark_asset_for_deletion(href)
        else:
            # Dataset already deleted in Dataspot, just log it
            logging.info(f"Dataset with ODS ID '{odsDataportalId}' (UUID: {uuid}) already deleted in Dataspot, updating local mapping only")
        
        # Remove entry from mapping in both cases
        self.mapping.remove_entry(odsDataportalId)
        
        return True
