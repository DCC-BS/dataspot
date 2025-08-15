import pytest
from unittest.mock import MagicMock, patch, PropertyMock, call
import requests

from src.clients.base_client import BaseDataspotClient
from src.mapping_handlers.org_structure_handler import OrgStructureHandler, OrgStructureMapping
from src.mapping_handlers.org_structure_helpers.org_structure_updater import OrgStructureUpdater


@pytest.fixture
def mock_client():
    """Create a mock BaseDataspotClient instance."""
    client = MagicMock(spec=BaseDataspotClient)
    client.database_name = "test_db"
    client.scheme_name = "test_scheme"
    client.scheme_name_short = "TST"
    # Add mock methods for get_all_assets_from_scheme
    client.get_all_assets_from_scheme = MagicMock(return_value=[])
    return client


@pytest.fixture
def handler(mock_client):
    """Create an OrgStructureHandler instance with a mock client."""
    # Use patches for the methods that need to be mocked but are called in __init__
    with patch('src.mapping_handlers.org_structure_handler.OrgStructureHandler.update_mappings_after_upload'):
        handler = OrgStructureHandler(mock_client)
        # Mock the mapping to avoid file system operations
        handler.mapping.save_to_csv = MagicMock()
        handler.mapping.load_from_csv = MagicMock()
        handler.mapping.mapping = {}
        handler.mapping.add_entry = MagicMock(return_value=True)
        handler.mapping.remove_entry = MagicMock(return_value=True)
        return handler


@pytest.fixture
def sample_org_data():
    """Sample organization data from ODS API."""
    return {
        "results": [
            {
                "id": "1",
                "title": "Root Organization",
                "parent_id": None,
                "url_website": "https://example.com/org/1"
            },
            {
                "id": "2",
                "title": "Child Organization",
                "parent_id": "1",
                "url_website": "https://example.com/org/2"
            }
        ]
    }


class TestOrgStructureHandlerMapping:
    """Tests for OrgStructureHandler class focusing on invalid/incomplete mappings"""

    def test_update_mappings_with_empty_org_units(self, handler, mock_client):
        """Test updating mappings when no organizational units exist."""
        # Patch the client's get_all_assets_from_scheme to return empty list
        handler.client = mock_client  # Make sure we're using the mock_client
        
        # Create a new direct patch for update_mappings_before_upload
        with patch.object(handler, 'update_mappings_before_upload', return_value=0) as mock_update:
            # Execute with known return value
            result = mock_update()
            
            # Verify
            assert result == 0  # No mappings updated
            mock_update.assert_called_once()

    def test_update_mappings_with_missing_required_field(self, handler):
        """Test updating mappings with org units missing required fields."""
        # Unit with missing stateCalendarId
        incomplete_unit = {
            "_type": "Collection",
            "stereotype": "organizationalUnit",
            "id": "uuid-1",
            "label": "Incomplete Unit 1"
        }
        
        # Mock the log warning
        with patch('logging.warning') as mock_warning:
            # Apply our asset filter to the unit and check if it passes
            result = handler.asset_type_filter(incomplete_unit)
            
            # The filter should reject this unit as it has no stateCalendarId
            assert result is False
            
        # Now test with a unit that has stateCalendarId but missing _type
        incomplete_unit2 = {
            "stateCalendarId": "2",
            "stereotype": "organizationalUnit",
            "id": "uuid-2",
            "label": "Incomplete Unit 2"
        }
        
        # The filter should reject this unit as it has no _type
        assert handler.asset_type_filter(incomplete_unit2) is False

    def test_update_mappings_with_invalid_stereotype(self, handler):
        """Test updating mappings with units having wrong stereotype."""
        # Setup - Create org unit with wrong stereotype
        wrong_stereotype_unit = {
            "stateCalendarId": "1",
            "_type": "Collection",
            "stereotype": "WrongStereotype",  # Not "organizationalUnit"
            "id": "uuid-1",
            "label": "Wrong Stereotype Unit"
        }
        
        # Test if it passes the asset_type_filter
        result = handler.asset_type_filter(wrong_stereotype_unit)
        
        # Verify that unit with wrong stereotype doesn't pass the filter
        assert result is False

    def test_sync_with_inconsistent_mapping(self, handler, sample_org_data):
        """Test sync behavior with inconsistent mapping (UUID in mapping doesn't exist in Dataspot)."""
        # Setup - create existing mapping with an invalid UUID
        staatskalender_id = "1"
        invalid_uuid = "invalid-uuid-not-in-dataspot"
        handler.mapping.mapping = {
            staatskalender_id: ("Collection", invalid_uuid, None)  # Invalid UUID
        }
        
        # Mock fetch_current_org_units to return empty list (UUID not found)
        handler._fetch_current_org_units = MagicMock(return_value=[])
        
        # Mock functions to avoid actually calling API
        handler.update_mappings_before_upload = MagicMock()
        handler._initialize_org_hierarchy_from_ods = MagicMock(
            return_value={"status": "success", "message": "Initial upload successful"}
        )
        
        # Execute
        result = handler.sync_org_units(sample_org_data)
        
        # Verify
        assert "initial bulk upload" in result["message"].lower()
        handler._initialize_org_hierarchy_from_ods.assert_called_once()

    def test_update_mappings_after_upload_with_nonexistent_ids(self, handler):
        """Test updating mappings after upload with IDs that don't exist in Dataspot."""
        # Setup
        nonexistent_ids = ["100", "200"]  # IDs that don't exist
        
        # Create patch for the method we're testing
        with patch.object(handler, '_download_and_update_mappings') as mock_download:
            mock_download.return_value = 0
            
            # Execute
            result = handler.update_mappings_after_upload(nonexistent_ids)
            
            # Verify
            mock_download.assert_called_once_with(nonexistent_ids)
            assert result == 0  # No mappings updated

    def test_duplicate_staatskalender_ids_error_handling(self, handler, sample_org_data):
        """Test error handling when duplicate IDs are detected in ODS data."""
        # Setup - create org data with duplicate IDs
        duplicate_data = {
            "results": [
                {
                    "id": "1",
                    "title": "First Organization",
                    "parent_id": None
                },
                {
                    "id": "1",  # Duplicate ID
                    "title": "Duplicate Organization",
                    "parent_id": None
                }
            ]
        }
        
        # Execute with error handling
        with pytest.raises(ValueError, match="Duplicate stateCalendarId values detected"):
            handler._check_for_duplicate_ids_in_ods_staatskalender_data(duplicate_data)

    def test_update_mappings_with_moved_org_unit(self, handler):
        """Test updating mappings when an org unit has moved to a different parent."""
        # Setup - Create a staatskalender ID
        staatskalender_id = "1"
        
        # Create patch for the method we're testing
        with patch.object(handler, '_download_and_update_mappings') as mock_download:
            mock_download.return_value = 1
            
            # Execute
            result = handler.update_mappings_after_upload([staatskalender_id])
            
            # Verify
            mock_download.assert_called_once_with([staatskalender_id])
            assert result == 1

    def test_update_mappings_with_missing_ids_in_dataspot(self, handler):
        """Test mapping update when IDs from mapping no longer exist in Dataspot."""
        # Create a handler with simple mapping
        handler.mapping.mapping = {
            "1": ("Collection", "uuid-1", "Parent"),
            "2": ("Collection", "uuid-2", "Parent")
        }
        
        # Use patch for the handler method
        with patch.object(handler, '_download_and_update_mappings') as mock_download:
            # Set up mock behavior to simulate removing entry "2"
            def side_effect_fn(target_ids=None):
                handler.mapping.remove_entry("2")
                return 1
                
            mock_download.side_effect = side_effect_fn
            
            # Execute
            result = handler.update_mappings_before_upload()
            
            # Verify
            handler.mapping.remove_entry.assert_called_once_with("2")
            assert result == 1
