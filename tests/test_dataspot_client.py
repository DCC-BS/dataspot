import json
import unittest
from unittest.mock import patch, MagicMock, call

import pytest
from requests import HTTPError

from src.dataspot_client import DataspotClient
from src.dataspot_dataset import Dataset


class TestDataspotClient(unittest.TestCase):
    """Test class for DataspotClient methods."""

    def setUp(self):
        """Set up test fixtures before each test."""
        # Create a patcher for the DataspotAuth class
        self.auth_patcher = patch('src.dataspot_client.DataspotAuth')
        self.mock_auth = self.auth_patcher.start()
        # Mock the headers returned by the auth
        self.mock_auth.return_value.get_headers.return_value = {'Authorization': 'Bearer fake-token'}
        
        # Mock environment variables
        self.env_patcher = patch('src.dataspot_client.os.getenv')
        self.mock_getenv = self.env_patcher.start()
        self.mock_getenv.return_value = 'https://test-dataspot-api.com'
        
        # Create the client
        self.client = DataspotClient()
        self.client.base_url = 'https://test-dataspot-api.com'
        self.client.database_name = 'test-db'
        self.client.dnk_scheme_name = 'Test-DNK'
        self.client.tdm_scheme_name = 'Test-TDM/collections/Test-Models'
        
        # Mock find_dataset_path and find_tdm_dataobject_path methods
        self.client.find_dataset_path = MagicMock(return_value='rest/test-db/datasets/test-dataset')
        self.client.find_tdm_dataobject_path = MagicMock(return_value='rest/test-db/schemes/Test-TDM/assets/test-dataset')
        
        # Sample dataset for testing
        self.sample_dataset = self._create_sample_dataset()
        
    def tearDown(self):
        """Tear down test fixtures after each test."""
        self.auth_patcher.stop()
        self.env_patcher.stop()
    
    def _create_sample_dataset(self):
        """Create a sample dataset for testing."""
        dataset = MagicMock(spec=Dataset)
        dataset.name = 'test-dataset'
        dataset.get_departement_dienststelle_sammlung_subsammlung.return_value = (
            'TestDept', 'TestDiv', 'TestColl', None
        )
        dataset.to_json.return_value = {
            '_type': 'TestDataset',
            'label': 'test-dataset',
            'title': 'Test Dataset',
            'description': 'Test dataset for unit testing'
        }
        return dataset
    
    def _mock_response(self, status_code=200, json_data=None, raise_for_status=None):
        """Create a mock response object for mocking requests."""
        mock_resp = MagicMock()
        mock_resp.status_code = status_code
        mock_resp.raise_for_status = MagicMock()
        
        if raise_for_status:
            mock_resp.raise_for_status.side_effect = raise_for_status
            
        if json_data:
            mock_json = MagicMock()
            mock_json.return_value = json_data
            mock_resp.json = mock_json
            
        return mock_resp
    
    def _setup_parent_collection_mock(self, requests_get_mock):
        """Setup mocks for parent collection resolution."""
        # Mock parent collection retrieval
        parent_collection_response = self._mock_response(
            json_data={
                'id': 'test-collection-uuid',
                '_links': {'self': {'href': '/rest/collections/test-collection-uuid'}}
            }
        )
        
        # Configure the mock to return 'test-collection-uuid' for the get('id') call
        parent_collection_response.json.return_value = {'id': 'test-collection-uuid'}
        
        requests_get_mock.return_value = parent_collection_response
        return parent_collection_response

    def _setup_dataset_exists_mock(self, requests_get_mock, exists=True):
        """Setup mocks for dataset existence check."""
        # Create parent collection response
        parent_collection_response = self._mock_response(
            json_data={
                'id': 'test-collection-uuid',
                '_links': {'self': {'href': '/rest/collections/test-collection-uuid'}}
            }
        )
        parent_collection_response.json.return_value = {'id': 'test-collection-uuid'}
        
        if exists:
            # Dataset exists response
            dataset_response = self._mock_response(
                json_data={
                    'id': 'test-dataset-uuid',
                    '_links': {'self': {'href': '/rest/collections/test-collection-uuid/assets/test-dataset'}}
                }
            )
            # Set up side effect for multiple calls
            requests_get_mock.side_effect = [parent_collection_response, dataset_response]
        else:
            # Dataset doesn't exist (404)
            error = HTTPError()
            error.response = MagicMock()
            error.response.status_code = 404
            
            # Set up side effect for multiple calls
            requests_get_mock.side_effect = [
                parent_collection_response,
                error,
                parent_collection_response  # For retry after creating hierarchy
            ]
        
        return requests_get_mock

    ###############
    # Tests for create_or_update_dataset method
    ###############
    
    @patch('src.dataspot_client.requests_get')
    @patch('src.dataspot_client.requests_post')
    def test_create_dataset_new(self, mock_post, mock_get):
        """Test creating a new dataset."""
        # Setup mocks
        self._setup_dataset_exists_mock(mock_get, exists=False)
        
        # Setup post response
        post_response = self._mock_response(
            json_data={
                'id': 'new-dataset-uuid',
                '_links': {'self': {'href': '/rest/collections/test-collection-uuid/assets/test-dataset'}}
            }
        )
        mock_post.return_value = post_response
        
        # Call the method
        result = self.client.dnk_create_or_update_dataset(self.sample_dataset, update_strategy='create_only')
        
        # Assertions
        mock_post.assert_called_once()
        self.assertEqual(result['id'], 'new-dataset-uuid')
    
    @patch('src.dataspot_client.requests_get')
    @patch('src.dataspot_client.requests_post')
    @patch('src.dataspot_client.requests_patch')
    def test_create_or_update_dataset_exists_patch(self, mock_patch, mock_post, mock_get):
        """Test updating an existing dataset with PATCH."""
        # Setup mocks
        self._setup_dataset_exists_mock(mock_get, exists=True)
        
        # Setup patch response
        patch_response = self._mock_response(
            json_data={
                'id': 'updated-dataset-uuid',
                '_links': {'self': {'href': '/rest/collections/test-collection-uuid/assets/test-dataset'}}
            }
        )
        mock_patch.return_value = patch_response
        
        # Call the method
        result = self.client.dnk_create_or_update_dataset(
            self.sample_dataset, 
            update_strategy='create_or_update', 
            force_replace=False
        )
        
        # Assertions
        mock_patch.assert_called_once()
        mock_post.assert_not_called()
        self.assertEqual(result['id'], 'updated-dataset-uuid')
    
    @patch('src.dataspot_client.requests_get')
    @patch('src.dataspot_client.requests_post')
    @patch('src.dataspot_client.requests_put')
    def test_create_or_update_dataset_exists_put(self, mock_put, mock_post, mock_get):
        """Test updating an existing dataset with PUT."""
        # Setup mocks
        self._setup_dataset_exists_mock(mock_get, exists=True)
        
        # Setup put response
        put_response = self._mock_response(
            json_data={
                'id': 'replaced-dataset-uuid',
                '_links': {'self': {'href': '/rest/collections/test-collection-uuid/assets/test-dataset'}}
            }
        )
        mock_put.return_value = put_response
        
        # Call the method
        result = self.client.dnk_create_or_update_dataset(
            self.sample_dataset, 
            update_strategy='create_or_update', 
            force_replace=True
        )
        
        # Assertions
        mock_put.assert_called_once()
        mock_post.assert_not_called()
        self.assertEqual(result['id'], 'replaced-dataset-uuid')
    
    @patch('src.dataspot_client.requests_get')
    @patch('src.dataspot_client.requests_post')
    def test_create_only_existing_dataset(self, mock_post, mock_get):
        """Test create_only with an existing dataset."""
        # Setup mocks
        self._setup_dataset_exists_mock(mock_get, exists=True)
        
        # Call the method
        result = self.client.dnk_create_or_update_dataset(
            self.sample_dataset, 
            update_strategy='create_only'
        )
        
        # Assertions
        mock_post.assert_not_called()
        self.assertIsNone(result)  # Method returns early when dataset exists
    
    @patch('src.dataspot_client.requests_get')
    @patch('src.dataspot_client.requests_patch')
    def test_update_only_nonexistent_dataset(self, mock_patch, mock_get):
        """Test update_only with a non-existent dataset."""
        # Setup mocks for non-existent dataset
        error = HTTPError()
        error.response = MagicMock()
        error.response.status_code = 404
        mock_get.side_effect = [
            # First call - parent collection exists
            self._mock_response(json_data={'id': 'test-collection-uuid'}),
            # Second call - dataset doesn't exist
            error
        ]
        
        # Call the method and assert it raises ValueError
        with self.assertRaises(ValueError):
            self.client.dnk_create_or_update_dataset(
                self.sample_dataset, 
                update_strategy='update_only'
            )
        
        # Assertions
        mock_patch.assert_not_called()
    
    @patch('src.dataspot_client.requests_get')
    def test_create_or_update_dataset_invalid_strategy(self, mock_get):
        """Test with an invalid update strategy."""
        with self.assertRaises(ValueError):
            self.client.dnk_create_or_update_dataset(
                self.sample_dataset, 
                update_strategy='invalid_strategy'
            )
        
        # Ensure no API calls were made
        mock_get.assert_not_called()
    
    @patch('src.dataspot_client.requests_get')
    def test_create_dataset_parent_not_exists(self, mock_get):
        """Test creating dataset when parent collection doesn't exist."""
        # Mock parent collection doesn't exist, then created
        parent_error = HTTPError()
        parent_error.response = MagicMock()
        parent_error.response.status_code = 404
        
        # Create a dataset error
        dataset_error = HTTPError()
        dataset_error.response = MagicMock()
        dataset_error.response.status_code = 404
        
        # First call - parent doesn't exist
        # Second call - parent exists after creation
        # Third call - dataset doesn't exist
        mock_get.side_effect = [
            parent_error,
            self._mock_response(json_data={'id': 'new-parent-uuid'}),
            dataset_error
        ]
        
        # Setup create_hierarchy_for_dataset as do-nothing mock
        self.client.create_hierarchy_for_dataset = MagicMock()
        
        # Setup post response for dataset creation
        with patch('src.dataspot_client.requests_post') as mock_post:
            post_response = self._mock_response(
                json_data={
                    'id': 'new-dataset-uuid',
                    '_links': {'self': {'href': '/rest/collections/test-collection-uuid/assets/test-dataset'}}
                }
            )
            mock_post.return_value = post_response
            
            # Call the method
            result = self.client.dnk_create_or_update_dataset(self.sample_dataset)
            
            # Assertions
            self.client.create_hierarchy_for_dataset.assert_called_once_with(self.sample_dataset)
            mock_post.assert_called_once()
    
    ###############
    # Tests for delete_dataset method
    ###############
    
    @patch('src.dataspot_client.requests_get')
    @patch('src.dataspot_client.requests_delete')
    def test_delete_existing_dataset(self, mock_delete, mock_get):
        """Test deleting an existing dataset."""
        # Setup mocks
        dataset_response = self._mock_response(
            json_data={
                'id': 'test-dataset-uuid',
                '_links': {'self': {'href': '/rest/test-db/datasets/test-dataset'}}
            }
        )
        
        tdm_response = self._mock_response(
            json_data={
                'id': 'test-tdm-uuid',
                '_links': {'self': {'href': '/rest/test-db/schemes/Test-TDM/assets/test-dataset'}}
            }
        )
        
        # First call - dataset exists
        # Second call - TDM asset exists
        mock_get.side_effect = [dataset_response, tdm_response]
        
        # Call the method
        result = self.client.delete_dataset('test-dataset')
        
        # Assertions
        self.assertTrue(result)
        self.assertEqual(mock_delete.call_count, 2)  # Dataset and TDM asset
    
    @patch('src.dataspot_client.requests_get')
    @patch('src.dataspot_client.requests_delete')
    def test_delete_nonexistent_dataset_no_fail(self, mock_delete, mock_get):
        """Test deleting a non-existent dataset with fail_if_not_exists=False."""
        # Setup mocks for non-existent dataset
        error = HTTPError()
        error.response = MagicMock()
        error.response.status_code = 404
        mock_get.side_effect = error
        
        # Call the method
        result = self.client.delete_dataset('nonexistent-dataset', fail_if_not_exists=False)
        
        # Assertions
        self.assertFalse(result)
        mock_delete.assert_not_called()
    
    @patch('src.dataspot_client.requests_get')
    @patch('src.dataspot_client.requests_delete')
    def test_delete_nonexistent_dataset_fail(self, mock_delete, mock_get):
        """Test deleting a non-existent dataset with fail_if_not_exists=True."""
        # Setup mocks for non-existent dataset
        error = HTTPError()
        error.response = MagicMock()
        error.response.status_code = 404
        mock_get.side_effect = error
        
        # Call the method and assert it raises ValueError
        with self.assertRaises(ValueError):
            self.client.delete_dataset('nonexistent-dataset', fail_if_not_exists=True)
        
        # Assertions
        mock_delete.assert_not_called()
    
    @patch('src.dataspot_client.requests_get')
    @patch('src.dataspot_client.requests_delete')
    def test_delete_dataset_without_tdm(self, mock_delete, mock_get):
        """Test deleting a dataset without deleting TDM asset."""
        # Setup mocks
        dataset_response = self._mock_response(
            json_data={
                'id': 'test-dataset-uuid',
                '_links': {'self': {'href': '/rest/test-db/datasets/test-dataset'}}
            }
        )
        mock_get.return_value = dataset_response
        
        # Call the method
        result = self.client.delete_dataset('test-dataset', delete_tdm_asset=False)
        
        # Assertions
        self.assertTrue(result)
        mock_delete.assert_called_once()  # Only dataset, not TDM
        
    @patch('src.dataspot_client.requests_get')
    @patch('src.dataspot_client.requests_delete')
    def test_delete_dataset_tdm_not_exists(self, mock_delete, mock_get):
        """Test deleting a dataset when the TDM asset doesn't exist."""
        # Setup mocks
        dataset_response = self._mock_response(
            json_data={
                'id': 'test-dataset-uuid',
                '_links': {'self': {'href': '/rest/test-db/datasets/test-dataset'}}
            }
        )
        
        # First call for dataset
        # Second call for TDM - 404
        tdm_error = HTTPError()
        tdm_error.response = MagicMock()
        tdm_error.response.status_code = 404
        
        mock_get.side_effect = [dataset_response, tdm_error]
        
        # Call the method
        result = self.client.delete_dataset('test-dataset')
        
        # Assertions
        self.assertTrue(result)
        mock_delete.assert_called_once()  # Only dataset deleted
    
    @patch('src.dataspot_client.requests_get')
    @patch('src.dataspot_client.requests_delete')
    def test_delete_dataset_other_http_error(self, mock_delete, mock_get):
        """Test deleting a dataset when a non-404 HTTP error occurs."""
        # Setup mocks for server error
        error = HTTPError()
        error.response = MagicMock()
        error.response.status_code = 500
        mock_get.side_effect = error
        
        # Call the method and assert it raises HTTPError
        with self.assertRaises(HTTPError):
            self.client.delete_dataset('error-dataset')
        
        # Assertions
        mock_delete.assert_not_called()
    
    @patch('src.dataspot_client.requests_get')
    @patch('src.dataspot_client.requests_delete')
    def test_delete_dataset_tdm_error(self, mock_delete, mock_get):
        """Test deleting a dataset when an error occurs deleting the TDM asset."""
        # Setup mocks
        dataset_response = self._mock_response(
            json_data={
                'id': 'test-dataset-uuid',
                '_links': {'self': {'href': '/rest/test-db/datasets/test-dataset'}}
            }
        )
        
        # TDM exists but error on delete
        tdm_response = self._mock_response(
            json_data={
                'id': 'test-tdm-uuid',
                '_links': {'self': {'href': '/rest/test-db/schemes/Test-TDM/assets/test-dataset'}}
            }
        )
        
        mock_get.side_effect = [dataset_response, tdm_response]
        
        # First delete succeeds (dataset), second fails (TDM)
        mock_delete.side_effect = [MagicMock(), HTTPError()]
        
        # Call the method
        result = self.client.delete_dataset('test-dataset')
        
        # Assertions - should still return True since dataset was deleted
        self.assertTrue(result)
        self.assertEqual(mock_delete.call_count, 2)

    @patch('src.dataspot_client.requests_get')
    @patch('src.dataspot_client.requests_post')
    def test_tdm_create_dataobject_new(self, mock_post, mock_get):
        """Test creating a new TDM dataobject when it doesn't exist."""
        # Setup mock for getting the collection
        collection_response = self._mock_response(json_data={'id': 'test-collection-uuid'})
        
        # Setup mock for checking if asset exists - 404 means it doesn't exist
        asset_not_found = HTTPError(response=MagicMock(status_code=404))
        
        # Setup mock for creating the asset
        asset_created_response = self._mock_response(json_data={
            '_links': {'self': {'href': '/rest/test-db/collections/test-collection-uuid/assets/test-dataobject'}}
        })
        
        # Configure mocks
        mock_get.side_effect = [collection_response, asset_not_found]
        mock_post.return_value = asset_created_response
        
        # Mock find_tdm_dataobject_path to raise ValueError (dataobject not found)
        self.client.find_tdm_dataobject_path = MagicMock(side_effect=ValueError("Dataobject not found"))
        
        # Sample columns for the dataobject
        columns = [
            {'label': 'ID', 'name': 'id', 'type': 'integer'},
            {'label': 'Name', 'name': 'name', 'type': 'string'}
        ]
        
        # Setup mock for ods_type_to_dataspot_uuid
        self.client.ods_type_to_dataspot_uuid = MagicMock(return_value='uuid-for-type')
        
        # Call the method
        self.client.tdm_create_or_update_dataobject('test-dataobject', columns)
        
        # Assertions - check if the collection was retrieved
        mock_get.assert_any_call(
            'https://test-dataspot-api.com/rest/test-db/schemes/Test-TDM/collections/Test-Models',
            headers={'Authorization': 'Bearer fake-token'},
            rate_limit_delay=1.0
        )
        
        # Check if the dataobject was created
        mock_post.assert_any_call(
            'https://test-dataspot-api.com/rest/test-db/collections/test-collection-uuid/assets',
            headers={'Authorization': 'Bearer fake-token'},
            json={'_type': 'UmlClass', 'label': 'test-dataobject'},
            rate_limit_delay=1.0
        )
        
        # Check if attributes were created
        for col in columns:
            mock_post.assert_any_call(
                'https://test-dataspot-api.com/rest/test-db/collections/test-collection-uuid/assets/test-dataobject/attributes',
                headers={'Authorization': 'Bearer fake-token'},
                json={
                    '_type': 'UmlAttribute',
                    'title': col['label'],
                    'label': col['name'],
                    'hasRange': 'uuid-for-type'
                },
                rate_limit_delay=1.0
            )
    
    @patch('src.dataspot_client.requests_get')
    @patch('src.dataspot_client.requests_post')
    @patch('src.dataspot_client.requests_patch')
    def test_tdm_update_dataobject_existing(self, mock_patch, mock_post, mock_get):
        """Test updating an existing TDM dataobject."""
        # Setup mock for getting the collection
        collection_response = self._mock_response(json_data={'id': 'test-collection-uuid'})
        
        # Setup mock for checking if asset exists
        asset_exists_response = self._mock_response(json_data={
            '_links': {'self': {'href': '/rest/test-db/collections/test-collection-uuid/assets/test-dataobject'}}
        })
        
        # Setup mock for getting existing attributes
        existing_attributes_response = self._mock_response(json_data={
            '_embedded': {
                'attributes': [
                    {
                        'label': 'id',
                        '_links': {'self': {'href': '/rest/test-db/attributes/attr-id-uuid'}}
                    }
                ]
            }
        })
        
        # Configure mocks
        mock_get.side_effect = [collection_response, asset_exists_response, existing_attributes_response]
        
        # Sample columns for the dataobject - one existing, one new
        columns = [
            {'label': 'ID', 'name': 'id', 'type': 'integer'},  # Existing attribute
            {'label': 'Name', 'name': 'name', 'type': 'string'}  # New attribute
        ]
        
        # Setup mock for ods_type_to_dataspot_uuid
        self.client.ods_type_to_dataspot_uuid = MagicMock(return_value='uuid-for-type')
        
        # Call the method
        self.client.tdm_create_or_update_dataobject('test-dataobject', columns)
        
        # Assertions - check if the existing attribute was updated
        mock_patch.assert_called_once_with(
            'https://test-dataspot-api.com/rest/test-db/attributes/attr-id-uuid',
            headers={'Authorization': 'Bearer fake-token'},
            json={
                '_type': 'UmlAttribute',
                'title': 'ID',
                'label': 'id',
                'hasRange': 'uuid-for-type'
            },
            rate_limit_delay=1.0
        )
        
        # Check if the new attribute was created
        mock_post.assert_called_once_with(
            'https://test-dataspot-api.com/rest/test-db/collections/test-collection-uuid/assets/test-dataobject/attributes',
            headers={'Authorization': 'Bearer fake-token'},
            json={
                '_type': 'UmlAttribute',
                'title': 'Name',
                'label': 'name',
                'hasRange': 'uuid-for-type'
            },
            rate_limit_delay=1.0
        )
    
    @patch('src.dataspot_client.requests_get')
    def test_tdm_create_dataobject_collection_not_found(self, mock_get):
        """Test handling of non-existent collection when creating a dataobject."""
        # Setup mock for collection not found
        collection_not_found = HTTPError(response=MagicMock(status_code=404))
        mock_get.side_effect = collection_not_found
        
        # Call the method and check if it raises the expected exception
        with self.assertRaises(HTTPError) as context:
            self.client.tdm_create_or_update_dataobject('test-dataobject', [{'label': 'ID', 'name': 'id', 'type': 'integer'}])
        
        # Verify the error message
        self.assertIn("Collection 'Test-TDM/collections/Test-Models' does not exist", str(context.exception))
    
    @patch('src.dataspot_client.requests_get')
    def test_tdm_create_dataobject_other_http_error(self, mock_get):
        """Test handling of other HTTP errors when creating a dataobject."""
        # Setup mock for other HTTP error (e.g., 500 server error)
        server_error = HTTPError(response=MagicMock(status_code=500))
        mock_get.side_effect = server_error
        
        # Call the method and check if it raises the expected exception
        with self.assertRaises(HTTPError):
            self.client.tdm_create_or_update_dataobject('test-dataobject', [{'label': 'ID', 'name': 'id', 'type': 'integer'}])
    
    @patch('src.dataspot_client.requests_get')
    @patch('src.dataspot_client.requests_post')
    def test_tdm_create_dataobject_json_decode_error(self, mock_post, mock_get):
        """Test handling of JSON decode errors when creating a dataobject."""
        # Setup mock for getting the collection
        collection_response = self._mock_response(json_data={'id': 'test-collection-uuid'})
        
        # Setup mock for checking if asset exists - 404 means it doesn't exist
        asset_not_found = HTTPError(response=MagicMock(status_code=404))
        
        # Setup mock for JSON decode error on post
        mock_get.side_effect = [collection_response, asset_not_found]
        mock_post.side_effect = json.JSONDecodeError("Invalid JSON", "", 0)
        
        # Mock find_tdm_dataobject_path to raise ValueError (dataobject not found)
        self.client.find_tdm_dataobject_path = MagicMock(side_effect=ValueError("Dataobject not found"))
        
        # Call the method and check if it raises the expected exception
        with self.assertRaises(json.JSONDecodeError):
            self.client.tdm_create_or_update_dataobject('test-dataobject', [{'label': 'ID', 'name': 'id', 'type': 'integer'}])
    
    @patch('src.dataspot_client.requests_get')
    @patch('src.dataspot_client.requests_post')
    def test_tdm_create_dataobject_no_columns(self, mock_post, mock_get):
        """Test creating a dataobject with no columns."""
        # Setup mock for getting the collection
        collection_response = self._mock_response(json_data={'id': 'test-collection-uuid'})
        
        # Setup mock for checking if asset exists - 404 means it doesn't exist
        asset_not_found = HTTPError(response=MagicMock(status_code=404))
        
        # Setup mock for creating the asset
        asset_created_response = self._mock_response(json_data={
            '_links': {'self': {'href': '/rest/test-db/collections/test-collection-uuid/assets/test-dataobject'}}
        })
        
        # Configure mocks
        mock_get.side_effect = [collection_response, asset_not_found]
        mock_post.return_value = asset_created_response
        
        # Mock find_tdm_dataobject_path to raise ValueError (dataobject not found)
        self.client.find_tdm_dataobject_path = MagicMock(side_effect=ValueError("Dataobject not found"))
        
        # Call the method with no columns
        self.client.tdm_create_or_update_dataobject('test-dataobject', None)
        
        # Verify that the dataobject was created but no attributes were added
        mock_get.assert_called()
        # The post should be called exactly once to create the dataobject
        self.assertEqual(mock_post.call_count, 1)
        mock_post.assert_called_once_with(
            'https://test-dataspot-api.com/rest/test-db/collections/test-collection-uuid/assets',
            headers={'Authorization': 'Bearer fake-token'},
            json={'_type': 'UmlClass', 'label': 'test-dataobject'},
            rate_limit_delay=1.0
        )
    
    @patch('src.dataspot_client.requests_get')
    @patch('src.dataspot_client.requests_post')
    @patch('src.dataspot_client.requests_patch')
    def test_tdm_update_dataobject_attribute_error(self, mock_patch, mock_post, mock_get):
        """Test handling of errors when updating attributes."""
        # Setup mock for getting the collection
        collection_response = self._mock_response(json_data={'id': 'test-collection-uuid'})
        
        # Setup mock for checking if asset exists
        asset_exists_response = self._mock_response(json_data={
            '_links': {'self': {'href': '/rest/test-db/collections/test-collection-uuid/assets/test-dataobject'}}
        })
        
        # Setup mock for getting existing attributes
        existing_attributes_response = self._mock_response(json_data={
            '_embedded': {
                'attributes': [
                    {
                        'label': 'id',
                        '_links': {'self': {'href': '/rest/test-db/attributes/attr-id-uuid'}}
                    }
                ]
            }
        })
        
        # Configure mocks
        mock_get.side_effect = [collection_response, asset_exists_response, existing_attributes_response]
        
        # Setup error for attribute update
        mock_patch.side_effect = Exception("Failed to update attribute")
        
        # Sample columns for the dataobject - one existing that will fail
        columns = [{'label': 'ID', 'name': 'id', 'type': 'integer'}]
        
        # Setup mock for ods_type_to_dataspot_uuid
        self.client.ods_type_to_dataspot_uuid = MagicMock(return_value='uuid-for-type')
        
        # Call the method and check if it raises the expected exception
        with self.assertRaises(Exception) as context:
            self.client.tdm_create_or_update_dataobject('test-dataobject', columns)
        
        # Verify the error message
        self.assertEqual(str(context.exception), "Failed to update attribute")


if __name__ == '__main__':
    pytest.main() 