import logging
from time import sleep

from src.ods_client import ODSClient
from src.clients.dnk_client import DNKClient
from dataspot_dataset import OGDDataset
import json

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
    main_2_test_ensure_ods_imports_collection_exists()
    logging.info('Job successful!')
    