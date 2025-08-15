import pytest
from typing import Dict, Any

from src.mapping_handlers.org_structure_helpers.org_structure_transformer import OrgStructureTransformer


@pytest.fixture
def sample_org_data() -> Dict[str, Any]:
    """Test fixture providing sample organization data."""
    return {
        "results": [
            {
                "id": "1",
                "title": "Root Organization",
                "parent_id": "",
                "url_website": "https://example.com/org/1"
            },
            {
                "id": "2",
                "title": "Child Organization 1",
                "parent_id": "1",
                "url_website": "https://example.com/org/2"
            },
            {
                "id": "3",
                "title": "Child Organization 2",
                "parent_id": "1",
                "url_website": "https://example.com/org/3"
            },
            {
                "id": "4",
                "title": "Grandchild Organization",
                "parent_id": "2",
                "url_website": "https://example.com/org/4"
            }
        ]
    }


def test_transform_to_layered_structure(sample_org_data):
    """Test the transform_to_layered_structure method."""
    # Transform the sample data
    layers = OrgStructureTransformer.transform_to_layered_structure(sample_org_data)
    
    # Verify the results
    assert len(layers) == 3, "Should have 3 depth layers (0, 1, 2)"
    assert 0 in layers, "Should have layer 0 (root)"
    assert 1 in layers, "Should have layer 1 (children)"
    assert 2 in layers, "Should have layer 2 (grandchildren)"
    
    # Check contents of each layer
    assert len(layers[0]) == 1, "Layer 0 should have 1 organization (root)"
    assert len(layers[1]) == 2, "Layer 1 should have 2 organizations (children)"
    assert len(layers[2]) == 1, "Layer 2 should have 1 organization (grandchild)"
    
    # Check specific IDs in each layer
    assert layers[0][0]["stateCalendarId"] == "1", "Root should be in layer 0"
    assert set(u["stateCalendarId"] for u in layers[1]) == {"2", "3"}, "Children should be in layer 1"
    assert layers[2][0]["stateCalendarId"] == "4", "Grandchild should be in layer 2"
    
    # Check hierarchy relationships are correctly set
    root_unit = layers[0][0]
    assert "inCollection" not in root_unit, "Root should not have inCollection"
    
    # Check that child organizations have correct inCollection paths
    child_units = layers[1]
    for child in child_units:
        assert "inCollection" in child, "Child should have inCollection"
        assert "Root Organization" in child["inCollection"], "Child should reference root"
    
    # Check that grandchild organization has correct inCollection path
    grandchild = layers[2][0]
    assert "inCollection" in grandchild, "Grandchild should have inCollection"
    assert "Child Organization 1" in grandchild["inCollection"], "Grandchild should reference correct parent"
    
    # Check custom properties
    for depth, units in layers.items():
        for unit in units:
            assert "customProperties" in unit, f"Unit at depth {depth} should have customProperties"
            assert "stateCalendarId" in unit["customProperties"], "Should have stateCalendarId in customProperties"
            assert "stateCalendarLink" in unit["customProperties"], "Should have stateCalendarLink in customProperties"