"""
One-off script: create/update the AUE BryteCube Datenprodukte in Dataspot.

Uploads the assets defined in scripts/brytcube_test.json (a "BryteCube" Collection
plus its Datasets/Datenprodukte) via Dataspot's bulk metadata upload/download API:
https://datenkatalog.bs.ch/api/{database}/manual/en/upload-download

Items in the JSON file that already carry an "id" are matched to the existing asset
and updated; items without an "id" (e.g. "Test-Datenprodukt-2") are created new.
Operation mode "ADD" only touches the assets listed in the file and leaves
everything else in the scheme untouched.
"""

import argparse
import json
import logging
import os
from typing import Any, Dict, List

import config
from src.clients.base_client import BaseDataspotClient

DEFAULT_UPLOAD_FILE = os.path.join(os.path.dirname(__file__), "brytcube_test.json")


def _extract_upload_errors(response_json: Any) -> List[str]:
    """Pull out ERROR-level messages from a bulk upload API response."""
    errors: List[str] = []
    if not isinstance(response_json, list):
        return errors
    for item in response_json:
        if not isinstance(item, dict):
            continue
        if (item.get("level") or "").upper() != "ERROR":
            continue
        message = (item.get("message") or "").strip()
        if message:
            errors.append(message)
    return errors


def upload_brytecube_datenprodukte(file_path: str = DEFAULT_UPLOAD_FILE,
                                    operation: str = "ADD",
                                    dry_run: bool = False,
                                    status: str = "WORKING") -> Dict[str, Any]:
    """
    Upload the BryteCube Datenprodukte JSON file to the Datenprodukte (DNK) scheme in Dataspot.

    Args:
        file_path: Path to the JSON file to upload. Defaults to scripts/brytcube_test.json.
        operation: Upload operation mode ("ADD", "REPLACE" or "FULL_LOAD"). Defaults to "ADD",
                   which only creates/updates the assets listed in the file.
        dry_run: Whether to perform a dry run without changing any data.
        status: Status to set on all uploaded assets. Defaults to "WORKING" (DRAFT group).

    Returns:
        dict: The JSON response from the upload API.

    Raises:
        ValueError: If the file does not contain a JSON list of assets.
        HTTPError: If the upload request fails.
    """
    with open(file_path, "r", encoding="utf-8") as f:
        assets = json.load(f)

    if not isinstance(assets, list):
        raise ValueError(f"Expected a JSON list of assets in {file_path}, got {type(assets)}")

    logging.info(f"Loaded {len(assets)} asset(s) from {file_path}")

    if config.database_name != "test-aue-brytecube-api":
        logging.warning(
            f"config.database_name is '{config.database_name}', not the expected "
            "'test-aue-brytecube-api'. Double-check config.py before proceeding."
        )

    client = BaseDataspotClient(scheme_name=config.dnk_scheme_name,
                                 scheme_name_short=config.dnk_scheme_name_short)

    logging.info(
        f"Uploading {len(assets)} asset(s) to scheme '{client.scheme_name}' in database "
        f"'{config.database_name}' (operation={operation}, dry_run={dry_run}, status={status})..."
    )

    response = client.bulk_create_or_update_assets(
        scheme_name=client.scheme_name,
        data=assets,
        operation=operation,
        dry_run=dry_run,
        status=status,
    )

    errors = _extract_upload_errors(response)
    if errors:
        logging.error(f"Upload completed with {len(errors)} error(s):")
        for error in errors:
            logging.error(f"  - {error}")
    else:
        logging.info("Upload completed without errors.")

    return response


def main():
    parser = argparse.ArgumentParser(
        description="Upload AUE BryteCube Datenprodukte to Dataspot via the bulk upload API."
    )
    parser.add_argument("--file", default=DEFAULT_UPLOAD_FILE,
                         help="Path to the JSON file to upload (default: scripts/brytcube_test.json)")
    parser.add_argument("--operation", default="ADD", choices=["ADD", "REPLACE", "FULL_LOAD"],
                         help="Upload operation mode. ADD (default) only creates/updates the assets "
                              "in the file. REPLACE/FULL_LOAD can mark/delete assets not included in "
                              "the file - use with care.")
    parser.add_argument("--dry-run", action="store_true",
                         help="Perform a dry run without changing any data.")
    parser.add_argument("--status", default="WORKING",
                         help="Status to set on all uploaded assets (default: WORKING).")
    args = parser.parse_args()

    response = upload_brytecube_datenprodukte(
        file_path=args.file,
        operation=args.operation,
        dry_run=args.dry_run,
        status=args.status,
    )
    logging.info(f"Response:\n{json.dumps(response, indent=2, ensure_ascii=False)}")


if __name__ == "__main__":
    if config.logging_for_prod:
        logging.basicConfig(level=logging.INFO)
    else:
        logging.basicConfig(
            level=logging.INFO,
            format="%(asctime)s [%(levelname)s] %(filename)s:%(lineno)d %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S",
        )
    logging.info(f"=== CURRENT DATABASE: {config.database_name} ===")
    logging.info(f"Executing {__file__}...")
    main()
