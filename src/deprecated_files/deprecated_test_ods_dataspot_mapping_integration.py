import logging
import os
import sys
import pytest
import ods_utils_py as ods_utils

from src.clients.dnk_client import DNKClient
from src.dataspot_dataset import Dataset
from src.deprecated_files.deprecated_metadata_translator_2 import ods_to_dataspot
from src.ods_dataspot_mapping import ODSDataspotMapping

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger("dataspot-debugger")


def retrieve_test_datasets() -> [Dataset]:
    """Create a test dataset with the given properties."""
    datasets = []

    ods_ids = ['100003', '100034', '100236']
    print(ods_ids)

    for ods_id in ods_ids:
        logging.info(f"Processing dataset {ods_id}...")

        ods_metadata = ods_utils.get_dataset_metadata(dataset_id=ods_id)
        dataset = ods_to_dataspot(ods_metadata=ods_metadata, ods_dataset_id=ods_id)
        datasets.append(dataset)

    return datasets


@pytest.fixture
def mapping_file():
    """Fixture to create a temporary mapping file for tests"""
    test_file = "test_debug_mapping.csv"
    if os.path.exists(test_file):
        os.remove(test_file)
    
    yield test_file
    
    # Cleanup after test
    if os.path.exists(test_file):
        os.remove(test_file)


@pytest.fixture
def dnk_mapping_file():
    """Fixture to create a temporary mapping file for DNK client tests"""
    test_mapping_file = "dnk_client_test_mapping.csv"
    if os.path.exists(test_mapping_file):
        os.remove(test_mapping_file)
    
    yield test_mapping_file
    
    # Note: We are not cleaning up the file here to allow manual inspection
    # Uncomment the following line if you want to clean up after tests
    # if os.path.exists(test_mapping_file):
    #     os.remove(test_mapping_file)


def test_mapping_operations(mapping_file):
    """Test basic mapping operations"""
    logger.info("Testing mapping operations...")

    mapping = ODSDataspotMapping(mapping_file)

    # Add some test entries
    mapping.add_entry("test-id-1", "test-uuid-1", "https://example.com/datasets/1")
    mapping.add_entry("test-id-2", "test-uuid-2", "https://example.com/datasets/2")

    # Verify entries
    assert mapping.get_uuid("test-id-1") == "test-uuid-1"
    assert mapping.get_href("test-id-2") == "https://example.com/datasets/2"

    # Test updating an entry
    mapping.add_entry("test-id-1", "updated-uuid-1", "https://example.com/updated/1")
    assert mapping.get_uuid("test-id-1") == "updated-uuid-1"

    # Test removing an entry
    assert mapping.remove_entry("test-id-2")
    assert mapping.get_entry("test-id-2") is None

    # Create new instance to test persistence
    new_mapping = ODSDataspotMapping(mapping_file)
    assert new_mapping.get_uuid("test-id-1") == "updated-uuid-1"
    assert new_mapping.get_entry("test-id-2") is None

    logger.info("Mapping operations test completed successfully")


@pytest.mark.integration
def test_dnk_client_operations(dnk_mapping_file):
    """Test DNK client operations with the mapping integration"""
    logger.info("Testing DNK client operations with mapping...")

    # Create client with faster request delay for testing
    client = DNKClient(request_delay=0.5, mapping_file=dnk_mapping_file)

    datasets = retrieve_test_datasets()

    for dataset in datasets:
        # Test creating new datasets
        try:
            logger.info(f"Creating dataset: {dataset.name}")
            response = client.create_or_update_dataset(dataset, update_strategy="create_or_update")
            logger.info(f"Successfully created dataset: {response.get('href')}")

            # TODO (Renato):
            # Test updating an existing dataset
            # Test create_or_update behavior in the case that it does not yet exist
            # Test create_or_update behavior in the case that it does already exist.
            # Test retrieving by href from mapping
            # Test persistence of mapping

            logger.info("DNK client operations test completed successfully")

        except Exception as e:
            logger.error(f"Error during DNK client test: {str(e)}")
            raise

        # Note: We are not cleaning up the datasets or mapping file here to allow manual inspection


if __name__ == "__main__":
    # This allows running the tests directly with python instead of pytest
    logger.info("Starting Dataspot debugging tests")

    # Test mapping operations first
    test_mapping_operations("test_debug_mapping.csv")

    # Then test DNK client with mapping
    test_dnk_client_operations("dnk_client_test_mapping.csv")

    logger.info("All tests completed") 