import logging
from time import sleep
import json

from src.ods_client import ODSClient
from src.clients.dnk_client import DNKClient
from src.dataspot_dataset import OGDDataset
import ods_utils_py as ods_utils

from src.dataset_transformer import transform_ods_to_dnk


def main_X():
    dataspot_client = DNKClient()
    ods_client = ODSClient()

    #ods_ids = ods_utils.get_all_dataset_ids(include_restricted=False)
    #ods_ids = ods_ids[:10]
    #ods_ids.append('100034') # Includes a ,
    #ods_ids.append('100236') # Includes a /
    ods_ids = ['100003', '100034', '100236']

    for ods_id in ods_ids:
        logging.info(f"Processing dataset {ods_id}...")

        ods_metadata = ods_utils.get_dataset_metadata(dataset_id = ods_id)
        dataset = transform_ods_to_dnk(ods_metadata=ods_metadata, ods_dataset_id=ods_id)

        #dataspot_client. ...
    pass

def main_1_test_require_scheme_exists():
    dataspot_client = DNKClient()

    dataspot_client.require_scheme_exists()

def main_2_test_ensure_ods_imports_collection_exists():
    dataspot_client = DNKClient()
    ods_imports_collection = dataspot_client.ensure_ods_imports_collection_exists()
    print(f"ODS-Imports collection: {ods_imports_collection}")

def main_3_test_create_or_update_dataset():
    """
    Test the create_or_update_dataset method of the DNKClient.
    
    This function:
    1. Creates a test OGDDataset
    2. Attempts to create it in Dataspot
    3. Modifies the dataset and updates it
    4. Tests different update strategies
    5. Deletes the dataset
    
    The results can be manually verified in the Dataspot UI.
    """
    logging.info("Testing create_or_update_dataset method...")
    
    
    # Initialize client
    dataspot_client = DNKClient()

    ods_ids = ['100003', '100034', '100236']

    # Create a test dataset
    test_dataset = OGDDataset(
        name="Test Dataset",
        beschreibung="This dataset was created to test the DNKClient's create_or_update_dataset method",
        schluesselwoerter=["test", "dataset", "api"],
        datenportal_identifikation="test-dataset-001",  # Required for ODS ID
        # Add required custom properties for OGD stereotype
        aktualisierungszyklus="http://publications.europa.eu/resource/authority/frequency/DAILY",
        tags=["test", "api", "documentation"]
    )
    
    # Test 1: Create a new dataset
    logging.info("Test 1: Creating a new dataset...")
    create_response = dataspot_client.create_or_update_dataset(
        dataset=test_dataset,
        update_strategy='create_only'
    )
    logging.info(f"Dataset created with UUID: {create_response.get('id')}")
    logging.info(f"Dataset href: {create_response.get('href')}")
    
    # Wait briefly to allow the server to process
    sleep(1)
    
    # Test 2: Update the dataset
    logging.info("Test 2: Updating the dataset...")
    test_dataset.beschreibung = "Updated test dataset description"
    test_dataset.tags = ["updated", "test"]
    
    update_response = dataspot_client.create_or_update_dataset(
        dataset=test_dataset,
        update_strategy='update_only'
    )
    logging.info(f"Dataset updated with UUID: {update_response.get('id')}")
    
    # Wait briefly to allow the server to process
    sleep(1)
    
    # Test 3: Create or update with force_replace
    logging.info("Test 3: Create or update with complete replacement...")
    test_dataset.beschreibung = "Complete replacement of the dataset"
    
    replace_response = dataspot_client.create_or_update_dataset(
        dataset=test_dataset,
        update_strategy='create_or_update',
        force_replace=True
    )
    logging.info(f"Dataset replaced with UUID: {replace_response.get('id')}")
    
    # Wait briefly to allow the server to process
    sleep(1)
    
    # Test 4: Delete the dataset
    logging.info("Test 4: Deleting the dataset...")
    delete_success = dataspot_client.delete_dataset(
        ods_id=test_dataset.datenportal_identifikation,
        fail_if_not_exists=True
    )
    logging.info(f"Dataset deletion successful: {delete_success}")
    
    logging.info("Tests completed. Check the Dataspot UI to verify all operations were performed correctly.")
    
    # Return the dataset ID for potential manual cleanup
    return test_dataset.datenportal_identifikation

def main_4_test_bulk_create_dataset():
    """
    Test the bulk_create_dataset method of the DNKClient.
    
    This function:
    1. Creates multiple test datasets
    2. Uploads them in a bulk operation to the scheme level, placing them in the ODS-Imports collection
    3. Tests different options (operations, dry run)
    4. Cleans up by deleting the datasets
    
    The results can be manually verified in the Dataspot UI.
    """
    logging.info("Testing bulk_create_dataset method...")
    
    # Initialize client
    dataspot_client = DNKClient()
    
    # Create test datasets
    test_datasets = []
    dataset_ids = []
    
    # Create 5 test datasets
    for i in range(1, 6):
        dataset_id = f"test-bulk-dataset-{i:03d}"
        dataset_ids.append(dataset_id)
        
        test_dataset = OGDDataset(
            name=f"Test Bulk Dataset {i}",
            beschreibung=f"This is test dataset #{i} for bulk upload testing",
            schluesselwoerter=["test", "bulk", "api"],
            datenportal_identifikation=dataset_id,  # Required for ODS ID
            # Add required custom properties for OGD stereotype
            aktualisierungszyklus="http://publications.europa.eu/resource/authority/frequency/DAILY",
            tags=["test", "bulk", f"dataset-{i}"]
        )
        test_datasets.append(test_dataset)
    
    try:
        # Test 1: Dry run with operation (ADD)
        logging.info("Test 1: Performing dry run with operation (ADD)...")
        dry_run_response = dataspot_client.bulk_create_or_update_datasets(
            datasets=test_datasets,
            operation="ADD",
            dry_run=True
        )
        logging.info(f"Dry run completed. Response: {dry_run_response}")
        
        # Wait briefly to allow the server to process
        sleep(2)
        
        # Test 2: Actual creation with operation (ADD)
        logging.info("Test 2: Bulk creating datasets with operation (ADD)...")
        create_response = dataspot_client.bulk_create_or_update_datasets(
            datasets=test_datasets,
            operation="ADD",
            dry_run=False
        )
        logging.info(f"Bulk creation completed. Response summary: {create_response}")
        
        # Wait briefly to allow the server to process
        sleep(2)
        
        # Test 3: Update with operation (REPLACE)
        # Modify some dataset properties
        for dataset in test_datasets:
            dataset.beschreibung = f"{dataset.beschreibung} - UPDATED"
            dataset.tags.append("updated")
        
        logging.info("Test 3: Updating datasets with operation (REPLACE)...")
        update_response = dataspot_client.bulk_create_or_update_datasets(
            datasets=test_datasets,
            operation="REPLACE",
            dry_run=False
        )
        logging.info(f"Bulk update completed. Response summary: {update_response}")
        
    finally:
        # Clean up: Delete all test datasets
        logging.info("Cleaning up: Deleting test datasets...")
        for dataset_id in dataset_ids:
            try:
                delete_success = dataspot_client.delete_dataset(
                    ods_id=dataset_id,
                    fail_if_not_exists=False
                )
                logging.info(f"Deleted dataset {dataset_id}: {delete_success}")
            except Exception as e:
                logging.warning(f"Failed to delete dataset {dataset_id}: {str(e)}")
        
    logging.info("Bulk dataset tests completed. Check the Dataspot UI to verify all operations were performed correctly.")
    
    return dataset_ids

def main_5_test_mapping_update():
    """
    Test the ability to update mappings after bulk dataset upload.

    This function:
    1. Creates multiple test datasets
    2. Uploads them using the bulk_create_dataset method
    3. Verifies that mappings are correctly updated
    4. Cleans up by deleting the test datasets
    """
    logging.info("Testing mapping update after bulk upload...")

    # Initialize client
    dataspot_client = DNKClient()

    # Create test datasets
    test_datasets = []
    dataset_ids = []

    # Create 3 test datasets with unique IDs
    for i in range(1, 4):
        dataset_id = f"test-mapping-{i:03d}"
        dataset_ids.append(dataset_id)

        test_dataset = OGDDataset(
            name=f"Test Mapping Dataset {i}",
            beschreibung=f"This is test dataset #{i} for mapping update testing",
            schluesselwoerter=["test", "mapping", "api"],
            datenportal_identifikation=dataset_id,  # Required for ODS ID
            aktualisierungszyklus="http://publications.europa.eu/resource/authority/frequency/DAILY",
            tags=["test", "mapping", f"dataset-{i}"]
        )
        test_datasets.append(test_dataset)

    try:
        # Upload the datasets in bulk
        logging.info("Creating datasets in bulk...")
        create_response = dataspot_client.bulk_create_or_update_datasets(
            datasets=test_datasets,
            operation="ADD",
            dry_run=False
        )
        logging.info(f"Bulk creation completed. Response: {create_response}")

        # Check if mappings were updated
        logging.info("Checking if mappings were updated...")
        mappings_updated = 0

        for dataset_id in dataset_ids:
            entry = dataspot_client.mapping.get_entry(dataset_id)
            if entry:
                uuid, href = entry
                logging.info(f"Mapping for {dataset_id}: UUID={uuid}, href={href}")
                mappings_updated += 1
            else:
                logging.warning(f"No mapping found for dataset ID: {dataset_id}")

        logging.info(f"Found mappings for {mappings_updated} out of {len(dataset_ids)} datasets")

    finally:
        # Clean up: Delete all test datasets
        logging.info("Cleaning up: Deleting test datasets...")
        for dataset_id in dataset_ids:
            try:
                delete_success = dataspot_client.delete_dataset(
                    ods_id=dataset_id,
                    fail_if_not_exists=False
                )
                logging.info(f"Deleted dataset {dataset_id}: {delete_success}")
            except Exception as e:
                logging.warning(f"Failed to delete dataset {dataset_id}: {str(e)}")

    logging.info("Mapping update test completed")
    return dataset_ids

def main_6_analyze_ods_metadata():
    """
    Analyzes and displays the structure of ODS metadata for a sample dataset.
    
    This function:
    1. Retrieves metadata for a specific dataset from ODS
    2. Prints the full metadata structure for analysis
    3. Tests the transform_ods_to_dnk function
    4. Displays the resulting OGDDataset
    """
    logging.info("Starting ODS metadata analysis...")

    dataset_id = '100397'
    
    # Get metadata from ODS
    logging.info(f"Retrieving metadata for dataset {dataset_id}...")
    ods_metadata = ods_utils.get_dataset_metadata(dataset_id=dataset_id)
    
    # Print the metadata structure
    logging.info("Full ODS metadata structure:")
    print(json.dumps(ods_metadata, indent=2))
    
    # Transform to Dataspot dataset
    logging.info("Transforming ODS metadata to Dataspot dataset...")
    dataset = transform_ods_to_dnk(ods_metadata=ods_metadata, ods_dataset_id=dataset_id)
    
    # Print the resulting dataset
    logging.info("Resulting OGDDataset:")
    print(f"Dataset name: {dataset.name}")
    print(f"Beschreibung: {dataset.beschreibung}")
    print(f"Schlüsselwörter: {dataset.schluesselwoerter}")
    print(f"Aktualisierungszyklus: {dataset.aktualisierungszyklus}")
    print(f"Publikationsdatum: {dataset.publikationsdatum}")
    print(f"Geographische Dimension: {dataset.geographische_dimension}")
    print(f"Datenportal-ID: {dataset.datenportal_identifikation}")
    print(f"Tags: {dataset.tags}")
    
    # Convert to JSON representation
    logging.info("Dataset JSON representation:")
    dataset_json = dataset.to_json()
    print(json.dumps(dataset_json, indent=2))
    
    logging.info("ODS metadata analysis completed")
    return dataset

def main_7_test_bulk_few_ods_datasets_upload():
    """
    Test the bulk upload of specific ODS datasets to Dataspot.
    
    This function:
    1. Retrieves metadata for specific ODS dataset IDs
    2. Transforms each dataset using transform_ods_to_dnk
    3. Creates a list of transformed datasets
    4. Calls bulk_create_or_update_datasets with this list
    5. Includes sleep statements for manual verification
    6. Cleans up datasets at the end
    
    Returns:
        List[str]: The list of dataset IDs that were uploaded
    """
    logging.info("Testing bulk upload of specific ODS datasets...")
    
    # Initialize clients
    dataspot_client = DNKClient()
    
    # Specific ODS dataset IDs to test
    ods_ids = ['100003', '100034', '100236', '100397']
    datasets = []
    
    # Retrieve and transform each dataset
    logging.info(f"Retrieving and transforming {len(ods_ids)} datasets...")
    for ods_id in ods_ids:
        try:
            logging.info(f"Processing dataset {ods_id}...")
            
            # Get metadata from ODS
            ods_metadata = ods_utils.get_dataset_metadata(dataset_id=ods_id)
            
            # Transform to Dataspot dataset
            dataset = transform_ods_to_dnk(ods_metadata=ods_metadata, ods_dataset_id=ods_id)
            
            # Add to list
            datasets.append(dataset)
            logging.info(f"Successfully transformed dataset {ods_id}: {dataset.name}")
            
        except Exception as e:
            logging.error(f"Error processing dataset {ods_id}: {str(e)}")
    
    if not datasets:
        logging.error("No datasets were successfully transformed. Aborting test.")
        return []
    
    dataset_ids = [dataset.datenportal_identifikation for dataset in datasets]
    logging.info(f"Successfully transformed {len(datasets)} datasets: {dataset_ids}")
    
    try:
        # Test 1: Dry run to check for any issues
        logging.info("Performing dry run of bulk upload...")
        dry_run_response = dataspot_client.bulk_create_or_update_datasets(
            datasets=datasets,
            operation="ADD",
            dry_run=True
        )
        logging.info(f"Dry run completed. Response: {dry_run_response}")
        
        # Wait for manual verification
        sleep(2)
        
        # Test 2: Actual creation with operation (ADD)
        logging.info("Bulk uploading datasets...")
        create_response = dataspot_client.bulk_create_or_update_datasets(
            datasets=datasets,
            operation="ADD",
            dry_run=False
        )
        logging.info(f"Bulk upload completed. Response summary: {create_response}")
        
        # Wait for manual verification
        sleep(3)
        
        # Test 3: Update with operation (REPLACE)
        # Modify some dataset properties for testing updates
        for dataset in datasets:
            dataset.beschreibung = f"{dataset.beschreibung} - UPDATED VIA BULK API"
            if dataset.tags:
                dataset.tags.append("bulk-updated")
            else:
                dataset.tags = ["bulk-updated"]
        
        logging.info("Updating datasets with operation (REPLACE)...")
        update_response = dataspot_client.bulk_create_or_update_datasets(
            datasets=datasets,
            operation="REPLACE",
            dry_run=False
        )
        logging.info(f"Bulk update completed. Response summary: {update_response}")
        
        # Wait for manual verification
        sleep(3)
        
        return dataset_ids
        
    finally:
        # Clean up: Delete all test datasets
        logging.info("Cleaning up: Deleting uploaded datasets...")
        for dataset_id in dataset_ids:
            try:
                delete_success = dataspot_client.delete_dataset(
                    ods_id=dataset_id,
                    fail_if_not_exists=False
                )
                logging.info(f"Deleted dataset {dataset_id}: {delete_success}")
            except Exception as e:
                logging.warning(f"Failed to delete dataset {dataset_id}: {str(e)}")
        logging.info("Cleanup completed")

def main_8_test_bulk_ods_datasets_upload():
    """
    Test the bulk upload of all public ODS datasets to Dataspot.
    
    This function:
    1. Retrieves all public dataset IDs from ODS
    2. Transforms each dataset using transform_ods_to_dnk
    3. Collects all datasets and uploads them at once
    4. Includes progress reporting with counters
    5. Provides options to limit the number of datasets processed
    
    Returns:
        List[str]: The list of dataset IDs that were uploaded
    """
    logging.info("Testing bulk upload of ALL public ODS datasets...")
    
    # Initialize clients
    dataspot_client = DNKClient()
    
    # Configuration
    max_datasets = None  # Maximum number of datasets to process (set to None for all)
    request_delay = 1.0  # Delay in seconds between API calls
    cleanup_after_test = True  # Whether to delete datasets after testing
    
    # Get all public dataset IDs
    logging.info("Retrieving all public dataset IDs...")
    ods_ids = ods_utils.get_all_dataset_ids(include_restricted=False)
    
    if max_datasets:
        ods_ids = ods_ids[:max_datasets]
        
    logging.info(f"Found {len(ods_ids)} datasets to process")
    
    # Process all datasets
    total_processed = 0
    total_successful = 0
    total_failed = 0
    processed_ids = []
    all_datasets = []
    
    # Process each dataset
    for idx, ods_id in enumerate(ods_ids):
        try:
            logging.info(f"[{idx+1}/{len(ods_ids)}] Processing dataset {ods_id}...")
            
            # Get metadata from ODS
            ods_metadata = ods_utils.get_dataset_metadata(dataset_id=ods_id)
            
            # Transform to Dataspot dataset
            dataset = transform_ods_to_dnk(ods_metadata=ods_metadata, ods_dataset_id=ods_id)
            
            # Add to collection
            all_datasets.append(dataset)
            processed_ids.append(ods_id)
            
            logging.info(f"Successfully transformed dataset {ods_id}: {dataset.name}")
            total_successful += 1
            
        except Exception as e:
            logging.error(f"Error processing dataset {ods_id}: {str(e)}")
            total_failed += 1
        
        total_processed += 1
    
    # Upload all datasets at once if we have any
    if all_datasets:
        try:
            # Bulk create all datasets
            logging.info(f"Bulk uploading all {len(all_datasets)} datasets...")
            
            # Perform the bulk upload
            create_response = dataspot_client.bulk_create_or_update_datasets(
                datasets=all_datasets,
                operation="ADD",
                dry_run=False
            )
            
            logging.info(f"Bulk upload completed. Response summary: {create_response}")
            
        except Exception as e:
            logging.error(f"Error during bulk upload: {str(e)}")
    
    logging.info(f"Completed processing {total_processed} datasets: {total_successful} successful, {total_failed} failed")
    
    # Clean up if requested
    if cleanup_after_test and processed_ids:
        logging.info(f"Cleaning up {len(processed_ids)} uploaded datasets...")
        
        for idx, dataset_id in enumerate(processed_ids):
            try:
                delete_success = dataspot_client.delete_dataset(
                    ods_id=dataset_id,
                    fail_if_not_exists=False
                )
                logging.info(f"[{idx+1}/{len(processed_ids)}] Deleted dataset {dataset_id}: {delete_success}")
                # Be kind to the server during cleanup
                sleep(request_delay)
            except Exception as e:
                logging.warning(f"[{idx+1}/{len(processed_ids)}] Failed to delete dataset {dataset_id}: {str(e)}")
        
        logging.info("Cleanup completed")
    
    return processed_ids

def main_X_build_organization_structure_in_dnk():
    """
    Build the organization structure in Dataspot's DNK scheme based on data from the ODS API.

    This method:
    1. Retrieves organization data from the ODS API
    2. Builds the organization hierarchy in Dataspot using a path-depth approach
       where organizations are processed level by level based on their path depth
    3. This ensures parent organizations are always created before their children
    """
    logging.info("Starting organization structure build...")

    # Initialize clients
    ods_client = ODSClient()
    dataspot_client = DataspotClient()

    # Configuration for data retrieval and processing
    batch_size = 100  # Number of records to retrieve in each API call
    max_batches = None  # Maximum number of batches to retrieve (set to None for all)
    cooldown_delay = 1.0  # Delay in seconds between API calls to prevent overloading the server
    all_organizations = {"results": []}

    # Fetch organization data in batches
    logging.info("Fetching organization data from ODS API...")
    batch_count = 0
    total_retrieved = 0

    try:
        while True:
            # Get the next batch of organization data
            offset = batch_count * batch_size
            batch_data = ods_client.get_organization_data(limit=batch_size, offset=offset)

            # Check if we received any results
            batch_results = batch_data.get('results', [])
            num_results = len(batch_results)

            if num_results == 0:
                # No more results, break out of the loop
                break

            # Add the batch results to our collected data
            all_organizations['results'].extend(batch_results)
            total_retrieved += num_results

            logging.info(
                f"Retrieved batch {batch_count + 1} with {num_results} organizations (total: {total_retrieved})")

            # Check if we've reached our batch limit
            batch_count += 1
            if max_batches is not None and batch_count >= max_batches:
                logging.info(f"Reached the maximum number of batches ({max_batches})")
                break

        # Set the total count in the combined data
        all_organizations['total_count'] = batch_data.get('total_count', total_retrieved)
        logging.info(f"Total organizations retrieved: {total_retrieved} (out of {all_organizations['total_count']})")

        # Optionally clear existing structure before building
        if True:  # Set to True to clear existing structure
            logging.info("Clearing existing organization structure...")
            dataspot_client.teardown_dnk(delete_empty_collections=True)

        # Build the organization hierarchy in Dataspot
        logging.info("Building organization hierarchy in Dataspot...")
        try:
            dataspot_client.build_organization_hierarchy_from_ods(
                all_organizations,
                cooldown_delay=cooldown_delay
            )
            logging.info("Organization structure build completed successfully")
        except Exception as e:
            logging.error(f"Error building organization hierarchy: {str(e)}")
            logging.info("Organization structure build partially completed with errors")

    except KeyboardInterrupt:
        logging.info("Operation was interrupted by user")
    except Exception as e:
        logging.error(f"Unexpected error: {str(e)}")
    finally:
        logging.info("Organization structure build process finished")

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    logging.info(f'Executing {__file__}...')
    main_8_test_bulk_ods_datasets_upload()
    logging.info('Job successful!')
    