import pytest
from unittest.mock import MagicMock, patch
import requests

from src.ods_client import ODSClient


@pytest.fixture
def client():
    """Create an ODSClient instance for testing."""
    return ODSClient()


@pytest.fixture
def sample_dataset_response():
    """Sample response from the dataset API."""
    return {
        "dataset_id": "100349",
        "dataset_uid": "test-uid",
        "fields": [
            {
                "name": "id",
                "label": "ID",
                "type": "text",
                "semantic_type": "identifier"
            },
            {
                "name": "title",
                "label": "Title",
                "type": "text"
            },
            {
                "name": "parent_id",
                "label": "Parent ID",
                "type": "text",
                "semantic_type": "identifier"
            }
        ]
    }


@pytest.fixture
def sample_org_data_response():
    """Sample response from the organization data API."""
    return {
        "total_count": 3,
        "results": [
            {
                "id": "1",
                "title": "Organization A",
                "parent_id": None,
                "url_website": "https://example.com/org/1",
                "children_id": ["2", "3"]
            },
            {
                "id": "2",
                "title": "Organization B",
                "parent_id": "1",
                "url_website": "https://example.com/org/2",
                "children_id": []
            }
        ]
    }


def test_get_dataset_columns(client, sample_dataset_response):
    """Test retrieving dataset columns."""
    with patch('ods_utils_py.requests_get') as mock_get:
        # Configure mock response
        mock_response = MagicMock()
        mock_response.json.return_value = sample_dataset_response
        mock_get.return_value = mock_response
        
        # Execute the function
        result = client.get_dataset_columns("100349")
        
        # Verify the results
        assert len(result) == 3
        assert result[0]["name"] == "id"
        assert result[0]["type"] == "identifier"  # Should convert text with identifier semantic_type
        assert result[1]["name"] == "title"
        assert result[1]["type"] == "text"
        
        # Verify the API call
        mock_get.assert_called_once()
        called_url = mock_get.call_args[1]['url']
        assert "100349" in called_url


def test_get_dataset_columns_missing_id(client):
    """Test error handling when dataset ID is missing."""
    with pytest.raises(SystemExit):
        client.get_dataset_columns(None)


def test_get_dataset_columns_api_error(client):
    """Test error handling for API errors."""
    with patch('ods_utils_py.requests_get') as mock_get:
        mock_response = MagicMock()
        mock_response.raise_for_status.side_effect = requests.HTTPError("API Error")
        mock_get.return_value = mock_response
        
        with pytest.raises(requests.HTTPError):
            client.get_dataset_columns("100349")


def test_get_organization_data(client, sample_org_data_response):
    """Test retrieving organization data."""
    with patch('ods_utils_py.requests_get') as mock_get:
        # Configure mock response
        mock_response = MagicMock()
        mock_response.json.return_value = sample_org_data_response
        mock_get.return_value = mock_response
        
        # Execute the function
        result = client._get_organization_data_batch(limit=50, offset=10)
        
        # Verify the results
        assert result == sample_org_data_response
        
        # Verify the API call
        mock_get.assert_called_once()
        called_url = mock_get.call_args[1]['url']
        assert "100349" in called_url
        called_params = mock_get.call_args[1]['params']
        assert called_params["limit"] == 50
        assert called_params["offset"] == 10


def test_get_organization_data_api_error(client):
    """Test error handling for API errors in get_organization_data."""
    with patch('ods_utils_py.requests_get') as mock_get:
        mock_response = MagicMock()
        mock_response.raise_for_status.side_effect = requests.HTTPError("API Error")
        mock_get.return_value = mock_response
        
        with pytest.raises(requests.HTTPError):
            client._get_organization_data_batch()


def test_get_all_organization_data(client, sample_org_data_response):
    """Test retrieving all organization data with batching."""
    with patch.object(client, 'get_organization_data') as mock_get_org:
        # First batch has data, second batch is empty (to end the loop)
        first_batch = sample_org_data_response
        empty_batch = {"results": [], "total_count": 3}
        
        mock_get_org.side_effect = [first_batch, empty_batch]
        
        # Execute the function
        result = client.get_all_organization_data(batch_size=2)
        
        # Verify the results
        assert len(result["results"]) == 2
        # The total_count comes from the last batch, which is the empty one in this case
        assert result["total_count"] == 3
        
        # Verify the API calls (two batches should have been requested)
        assert mock_get_org.call_count == 2
        assert mock_get_org.call_args_list[0][1]["limit"] == 2
        assert mock_get_org.call_args_list[0][1]["offset"] == 0
        assert mock_get_org.call_args_list[1][1]["limit"] == 2
        assert mock_get_org.call_args_list[1][1]["offset"] == 2


def test_get_all_organization_data_max_batches(client, sample_org_data_response):
    """Test limiting the number of batches in get_all_organization_data."""
    with patch.object(client, 'get_organization_data') as mock_get_org:
        # Configure mock to always return data (but we'll limit to max_batches)
        mock_get_org.return_value = sample_org_data_response
        
        # Execute with max_batches=2
        result = client.get_all_organization_data(batch_size=10, max_batches=2)
        
        # Verify that only 2 batches were requested
        assert mock_get_org.call_count == 2
        assert len(result["results"]) == 4  # 2 items per batch * 2 batches


def test_get_all_organization_data_api_error(client):
    """Test error handling for API errors in get_all_organization_data."""
    with patch.object(client, 'get_organization_data') as mock_get_org:
        mock_get_org.side_effect = requests.HTTPError("API Error")
        
        with pytest.raises(requests.HTTPError):
            client.get_all_organization_data()


def test_empty_response_handling(client):
    """Test handling of empty responses from the API."""
    with patch('ods_utils_py.requests_get') as mock_get:
        # Empty response from the API
        mock_response = MagicMock()
        mock_response.json.return_value = {"results": []}
        mock_get.return_value = mock_response
        
        # Execute the function
        result = client._get_organization_data_batch()
        
        # Verify the results
        assert result == {"results": []}


def test_malformed_response_handling(client):
    """Test handling of malformed responses from the API."""
    with patch('ods_utils_py.requests_get') as mock_get:
        # Malformed response missing 'fields'
        mock_response = MagicMock()
        mock_response.json.return_value = {"dataset_id": "100349"}  # Missing 'fields'
        mock_get.return_value = mock_response
        
        # Should not raise an exception, just return empty list
        result = client.get_dataset_columns("100349")
        assert result == [] 