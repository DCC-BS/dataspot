#!/usr/bin/env python
"""
Test script for the legal-basis status decision tree in scripts/refresh_vvp_legal_basis_status.py
"""

import sys
import os
from unittest.mock import MagicMock, patch

import pytest

# Add the parent directory to the path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from scripts.refresh_vvp_legal_basis_status import (
    PUBLISHED_STATUS,
    QUALIFIER_PROPERTY,
    STATUS_CHANGED,
    STATUS_CURRENT,
    STATUS_REPEALED,
    STATUS_UNKNOWN,
    compute_legal_basis_status,
)
from src.clients.vvp_client import VVPClient


class TestComputeLegalBasisStatus:
    """Test cases for compute_legal_basis_status."""

    def test_target_not_published_is_repealed(self):
        assert (
            compute_legal_basis_status(
                target_status="DELETENEW",
                version_active_since_ms=1577836800000,
                current_as_of_ms=1609459200000,
            )
            == STATUS_REPEALED
        )

    def test_target_not_published_is_repealed_even_without_date(self):
        assert (
            compute_legal_basis_status(
                target_status="WORKING",
                version_active_since_ms=None,
                current_as_of_ms=1609459200000,
            )
            == STATUS_REPEALED
        )

    def test_published_without_version_active_since_is_unknown(self):
        assert (
            compute_legal_basis_status(
                target_status=PUBLISHED_STATUS,
                version_active_since_ms=None,
                current_as_of_ms=1609459200000,
            )
            == STATUS_UNKNOWN
        )

    def test_version_active_since_after_current_as_of_is_changed(self):
        assert (
            compute_legal_basis_status(
                target_status=PUBLISHED_STATUS,
                version_active_since_ms=1640995200000,  # 2022-01-01
                current_as_of_ms=1609459200000,  # 2021-01-01
            )
            == STATUS_CHANGED
        )

    def test_version_active_since_before_current_as_of_is_current(self):
        assert (
            compute_legal_basis_status(
                target_status=PUBLISHED_STATUS,
                version_active_since_ms=1577836800000,  # 2020-01-01
                current_as_of_ms=1609459200000,  # 2021-01-01
            )
            == STATUS_CURRENT
        )

    def test_version_active_since_equal_to_current_as_of_is_current(self):
        assert (
            compute_legal_basis_status(
                target_status=PUBLISHED_STATUS,
                version_active_since_ms=1609459200000,
                current_as_of_ms=1609459200000,
            )
            == STATUS_CURRENT
        )


class TestPatchUsage:
    def test_puts_qualifier_without_type_and_keeps_existing_links(self):
        with patch.object(VVPClient, "__init__", lambda self: None):
            client = VVPClient()
        client.auth = MagicMock()
        client.auth.get_headers.return_value = {"Authorization": "Bearer test"}

        get_response = MagicMock()
        get_response.status_code = 200
        get_response.json.return_value = {
            "id": "usage-1",
            "usedBy": "processing-1",
            "usageOf": "law-1",
            "_type": "Usage",
        }
        put_response = MagicMock()
        put_response.json.return_value = {
            "id": "usage-1",
            "qualifier": STATUS_CHANGED,
            "usedBy": "processing-1",
            "usageOf": "law-1",
        }

        with patch("src.clients.vvp_client.requests_get", return_value=get_response) as mock_get:
            with patch(
                "src.clients.vvp_client.requests_put_no_retry", return_value=put_response
            ) as mock_put:
                with patch("src.clients.vvp_client.requests_patch_no_retry") as mock_patch:
                    result = client.patch_usage(
                        usage_uuid="usage-1",
                        payload={QUALIFIER_PROPERTY: STATUS_CHANGED},
                    )

        mock_get.assert_called_once()
        mock_patch.assert_not_called()
        mock_put.assert_called_once()
        sent_payload = mock_put.call_args.kwargs["json"]
        assert sent_payload == {
            QUALIFIER_PROPERTY: STATUS_CHANGED,
            "usedBy": "processing-1",
            "usageOf": "law-1",
        }
        assert "_type" not in sent_payload
        assert result[QUALIFIER_PROPERTY] == STATUS_CHANGED


# This allows running the test file directly
if __name__ == "__main__":
    pytest.main(["-v", __file__])
