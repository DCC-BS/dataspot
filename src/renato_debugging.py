import logging
import os
import sys
from typing import Dict, Any

from src.clients.dnk_client import DNKClient
from src.dataspot_dataset import Dataset, OGDDataset
from src.ods_dataspot_mapping import ODSDataspotMapping

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger("dataspot-debugger")


def create_test_dataset(ods_id: str, title: str, description: str) -> Dataset:
    """Create a test dataset with the given properties."""
    # TODO (in the distant future, BEFORE any release): Remove deprecated _PATH method (AND ALL RELATED AND DEPENDENT CODE)
    dataset = OGDDataset(
        name="OGD-Dataset-TEST",
        _PATH="Test-Departement/Test-Dienststelle/Test-Sammlung",
        kurzbeschreibung=title,
        beschreibung=description,
        synonyme=["Test-Synonym", "Test-Synonym-2"],
        schluesselwoerter=["Abfall", "Abort", "Abstimmung"]
    )
    return dataset


def test_mapping_operations():
    """Test basic mapping operations"""
    logger.info("Testing mapping operations...")
    
    # Use a temporary file for testing
    test_file = "test_debug_mapping.csv"
    if os.path.exists(test_file):
        os.remove(test_file)
    
    mapping = ODSDataspotMapping(test_file)
    
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
    new_mapping = ODSDataspotMapping(test_file)
    assert new_mapping.get_uuid("test-id-1") == "updated-uuid-1"
    assert new_mapping.get_entry("test-id-2") is None
    
    # Clean up
    os.remove(test_file)
    logger.info("Mapping operations test completed successfully")


def test_dnk_client_operations():
    """Test DNK client operations with the mapping integration"""
    logger.info("Testing DNK client operations with mapping...")
    
    # Use a dedicated file for this test
    test_mapping_file = "dnk_client_test_mapping.csv"
    if os.path.exists(test_mapping_file):
        os.remove(test_mapping_file)
    
    # Create client with faster request delay for testing
    client = DNKClient(request_delay=0.5, mapping_file=test_mapping_file)
    
    # Create test datasets
    dataset1 = create_test_dataset(
        "test-ods-1",
        "Test Dataset 1",
        "This is a test dataset for debugging the DNKClient"
    )
    
    dataset2 = create_test_dataset(
        "test-ods-2", 
        "Test Dataset 2",
        "This is another test dataset with a different ID"
    )
    
    # Test creating new datasets
    try:
        logger.info(f"Creating dataset: {dataset1.title}")
        response1 = client.create_or_update_dataset(dataset1, update_strategy="create_only")
        logger.info(f"Successfully created dataset: {response1.get('href')}")
        
        logger.info(f"Creating dataset: {dataset2.title}")
        response2 = client.create_or_update_dataset(dataset2, update_strategy="create_only")
        logger.info(f"Successfully created dataset: {response2.get('href')}")
        
        # Verify mapping was updated
        assert client.mapping.has_entry("test-ods-1")
        assert client.mapping.has_entry("test-ods-2")
        
        # Test updating an existing dataset
        logger.info(f"Updating dataset: {dataset1.title}")
        dataset1.description = "Updated description for testing"
        update_response = client.create_or_update_dataset(dataset1, update_strategy="update_only")
        logger.info(f"Successfully updated dataset: {update_response.get('href')}")
        
        # Test create_or_update behavior
        logger.info(f"Creating or updating dataset: {dataset2.title}")
        dataset2.description = "Changed description for testing create_or_update"
        update_response2 = client.create_or_update_dataset(dataset2)  # default is create_or_update
        logger.info(f"Successfully created or updated dataset: {update_response2.get('href')}")
        
        # Test retrieving by href from mapping
        href1 = client.mapping.get_href("test-ods-1")
        logger.info(f"Retrieved href from mapping: {href1}")
        
        # Test persistence of mapping
        new_client = DNKClient(mapping_file=test_mapping_file)
        assert new_client.mapping.get_uuid("test-ods-1") == client.mapping.get_uuid("test-ods-1")
        assert new_client.mapping.get_href("test-ods-2") == client.mapping.get_href("test-ods-2")
        
        logger.info("DNK client operations test completed successfully")
        
    except Exception as e:
        logger.error(f"Error during DNK client test: {str(e)}")
        raise
    
    # Note: We are not cleaning up the datasets or mapping file here to allow manual inspection


if __name__ == "__main__":
    logger.info("Starting Dataspot debugging tests")
    
    # Test mapping operations first
    test_mapping_operations()
    
    # Then test DNK client with mapping
    test_dnk_client_operations()
    
    logger.info("All tests completed")

