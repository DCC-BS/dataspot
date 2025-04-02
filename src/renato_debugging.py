import logging
from time import sleep

from src.ods_client import ODSClient
from src.clients.dnk_client import DNKClient
from dataspot_dataset import OGDDataset

import ods_utils_py as ods_utils

from deprecated_metadata_translator import ods_to_dataspot


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
        dataset = ods_to_dataspot(ods_metadata=ods_metadata, ods_dataset_id=ods_id, dataspot_client=dataspot_client)

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
        dry_run_response = dataspot_client.bulk_create_dataset(
            datasets=test_datasets,
            operation="ADD",
            dry_run=True
        )
        logging.info(f"Dry run completed. Response: {dry_run_response}")
        
        # Wait briefly to allow the server to process
        sleep(2)
        
        # Test 2: Actual creation with operation (ADD)
        logging.info("Test 2: Bulk creating datasets with operation (ADD)...")
        create_response = dataspot_client.bulk_create_dataset(
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
        update_response = dataspot_client.bulk_create_dataset(
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
    main_4_test_bulk_create_dataset()
    logging.info('Job successful!')
    