import datetime
import html
import json
import logging
import os
import re
import traceback
from typing import Any, Dict, List

import config
import ods_utils_py as ods_utils
from src.clients.law_client import LAWClient


ODS_DATASET_ID = "100354"
ODS_BATCH_SIZE = 100
WRITE_STATUS = "WORKING"


def normalize_systematic_number(value: Any) -> str:
    """Normalize systematic_number by trimming and removing wrapping quotes."""
    if value is None:
        return ""
    normalized = str(value).strip()
    while len(normalized) >= 2 and normalized[0] == normalized[-1] and normalized[0] in ("'", '"'):
        normalized = normalized[1:-1].strip()
    return normalized


def _strip_html(raw: str) -> str:
    raw = re.sub(r"<[^>]+>", "", raw or "")
    raw = html.unescape(raw)
    raw = raw.replace("\xa0", " ")
    return " ".join(raw.split()).strip()


def parse_paragraphs_from_gesetzestext_html(gesetzestext_html: str) -> List[Dict[str, str]]:
    """
    Parse paragraph entries from law html.
    Paragraph code is the number after article_symbol; shortText is article title.
    """
    if not gesetzestext_html:
        return []

    article_pattern = re.compile(
        r"<div class=['\"]article['\"]>\s*"
        r"<div class=['\"]article_number['\"]>.*?<span class=['\"]article_symbol['\"]>.*?</span>\s*"
        r"<span class=['\"]number['\"]>(?P<code>.*?)</span>.*?</div>\s*"
        r"<div class=['\"]article_title['\"]>.*?<span class=['\"]title_text['\"]>(?P<title>.*?)</span>.*?</div>\s*"
        r"</div>",
        re.DOTALL | re.IGNORECASE,
    )

    paragraphs: List[Dict[str, str]] = []
    seen_codes: set[str] = set()
    for match in article_pattern.finditer(gesetzestext_html):
        code = _strip_html(match.group("code"))
        if not code or code in seen_codes:
            continue

        short_text = _strip_html(match.group("title"))
        if short_text in {"§", "�"}:
            short_text = ""

        paragraphs.append({"code": code, "shortText": short_text})
        seen_codes.add(code)

    return paragraphs


def fetch_active_laws_from_ods(batch_size: int = ODS_BATCH_SIZE) -> List[Dict[str, Any]]:
    """
    Read active+current law records from ODS dataset 100354 using pagination.
    """
    laws: List[Dict[str, Any]] = []
    offset = 0
    while True:
        response = ods_utils.requests_get(
            url=f"https://data.bs.ch/api/explore/v2.1/catalog/datasets/{ODS_DATASET_ID}/records",
            params={
                "where": "is_active=true AND info_badge='current'",
                "order_by": "systematic_number",
                "limit": batch_size,
                "offset": offset,
            },
        )
        response.raise_for_status()
        payload = response.json()
        batch = payload.get("results", [])
        if not batch:
            break

        laws.extend(batch)
        logging.info(
            f"Retrieved {len(batch)} ODS law records (offset={offset}, total_collected={len(laws)})"
        )
        offset += batch_size

    logging.info(f"Retrieved {len(laws)} total active/current laws from ODS")
    return laws


def build_law_cache(
    assets: List[Dict[str, Any]], law_collection_uuid: str, law_client: LAWClient
) -> Dict[str, Dict[str, Any]]:
    """
    Build immutable run cache keyed by normalized systematic_number.
    """
    by_law_id: Dict[str, Dict[str, Any]] = {}
    by_systematic_number: Dict[str, Dict[str, Any]] = {}

    for asset in assets:
        if asset.get("_type") != "ReferenceObject":
            continue
        if asset.get("stereotype") != "LAW":
            continue
        if asset.get("inCollection") != law_collection_uuid:
            continue

        systematic_number = normalize_systematic_number(
            law_client.get_custom_property(asset, "systematic_number")
        )
        if not systematic_number:
            continue

        law_entry = {
            "id": asset.get("id"),
            "label": asset.get("label", ""),
            "description": asset.get("description", ""),
            "stereotype": asset.get("stereotype", ""),
            "systematic_number": systematic_number,
            "values_by_code": {},
        }
        by_law_id[asset.get("id")] = law_entry
        by_systematic_number[systematic_number] = law_entry

    for asset in assets:
        if asset.get("_type") != "ReferenceValue":
            continue
        parent_id = law_client.get_literal_parent_id(asset)
        if parent_id not in by_law_id:
            continue

        code = str(asset.get("code", "")).strip()
        if not code:
            continue

        by_law_id[parent_id]["values_by_code"][code] = {
            "id": asset.get("id"),
            "code": code,
            "shortText": asset.get("shortText", "") or "",
        }

    logging.info(
        f"Built LAW cache with {len(by_systematic_number)} ReferenceObjects from Download API"
    )
    return by_systematic_number


def build_reference_object_payload(
    systematic_number: str, title_de: str, original_url_de: str
) -> Dict[str, Any]:
    return {
        "_type": "ReferenceObject",
        "label": f"SG {systematic_number} - {title_de}",
        "stereotype": "LAW",
        "description": original_url_de or "",
        "customProperties": {
            "systematic_number": systematic_number,
        },
    }


def build_reference_value_payload(code: str, short_text: str) -> Dict[str, Any]:
    return {
        "_type": "ReferenceValue",
        "code": code,
        "shortText": short_text if short_text else "",
    }


def sync_law_bs() -> Dict[str, Any]:
    logging.info("Starting Basel-Stadt law sync")

    report = {
        "status": "pending",
        "counts": {
            "laws_created": 0,
            "laws_updated": 0,
            "laws_unchanged": 0,
            "values_created": 0,
            "values_updated": 0,
            "values_unchanged": 0,
            "errors": 0,
        },
        "errors": [],
    }

    law_client = LAWClient()
    try:
        ods_laws = fetch_active_laws_from_ods()
        scheme_assets = law_client.download_scheme_assets()
        law_collection_uuid = law_client.resolve_collection_uuid_by_label(
            scheme_assets, config.law_bs_collection_label
        )
        logging.info(f"Resolved LAW target collection UUID: {law_collection_uuid}")

        law_cache = build_law_cache(
            assets=scheme_assets, law_collection_uuid=law_collection_uuid, law_client=law_client
        )

        for record in ods_laws:
            systematic_number = normalize_systematic_number(record.get("systematic_number"))
            title_de = (record.get("title_de") or "").strip()
            original_url_de = (record.get("original_url_de") or "").strip()
            paragraphs = parse_paragraphs_from_gesetzestext_html(record.get("gesetzestext_html", ""))

            if not systematic_number or not title_de:
                report["counts"]["errors"] += 1
                error_msg = (
                    f"Skipping record due to missing required field(s): "
                    f"systematic_number='{systematic_number}', title_de='{title_de}'"
                )
                report["errors"].append(error_msg)
                logging.error(error_msg)
                continue

            desired_law = build_reference_object_payload(
                systematic_number=systematic_number,
                title_de=title_de,
                original_url_de=original_url_de,
            )

            existing_law = law_cache.get(systematic_number)
            current_law_id = None
            current_values_by_code: Dict[str, Dict[str, Any]] = {}

            if not existing_law:
                created_law = law_client.create_reference_object(
                    collection_uuid=law_collection_uuid, data=desired_law, status=WRITE_STATUS
                )
                current_law_id = created_law.get("id")
                report["counts"]["laws_created"] += 1
                logging.info(
                    f"Created law '{desired_law['label']}' with systematic_number={systematic_number}"
                )
            else:
                current_law_id = existing_law.get("id")
                current_values_by_code = existing_law.get("values_by_code", {})

                law_changed = (
                    existing_law.get("label") != desired_law["label"]
                    or (existing_law.get("description") or "") != desired_law["description"]
                    or existing_law.get("stereotype") != desired_law["stereotype"]
                    or normalize_systematic_number(existing_law.get("systematic_number"))
                    != normalize_systematic_number(
                        desired_law.get("customProperties", {}).get("systematic_number")
                    )
                )

                if law_changed:
                    law_client.update_reference_object(
                        law_id=current_law_id, data=desired_law, status=WRITE_STATUS
                    )
                    report["counts"]["laws_updated"] += 1
                    logging.info(
                        f"Updated law '{desired_law['label']}' with systematic_number={systematic_number}"
                    )
                else:
                    report["counts"]["laws_unchanged"] += 1

            if not current_law_id:
                report["counts"]["errors"] += 1
                error_msg = (
                    f"Cannot sync literals because law id is missing for systematic_number={systematic_number}"
                )
                report["errors"].append(error_msg)
                logging.error(error_msg)
                continue

            for paragraph in paragraphs:
                desired_value = build_reference_value_payload(
                    code=paragraph["code"],
                    short_text=paragraph["shortText"],
                )
                existing_value = current_values_by_code.get(desired_value["code"])

                if not existing_value:
                    law_client.create_reference_value(
                        law_id=current_law_id, data=desired_value, status=WRITE_STATUS
                    )
                    report["counts"]["values_created"] += 1
                    logging.info(
                        f"Created literal code={desired_value['code']} for law systematic_number={systematic_number}"
                    )
                    continue

                value_changed = (
                    (existing_value.get("code") or "") != desired_value["code"]
                    or (existing_value.get("shortText") or "") != desired_value["shortText"]
                )

                if value_changed:
                    law_client.update_reference_value(
                        value_id=existing_value.get("id"),
                        data=desired_value,
                        status=WRITE_STATUS,
                    )
                    report["counts"]["values_updated"] += 1
                    logging.info(
                        f"Updated literal code={desired_value['code']} for law systematic_number={systematic_number}"
                    )
                else:
                    report["counts"]["values_unchanged"] += 1

            # TODO: Future reconciliation step - detect and remove/mark obsolete literals for each law.

        # TODO: Future reconciliation step - detect and remove/mark obsolete laws absent from ODS.

        report["status"] = "success"
    except Exception as exc:
        report["status"] = "error"
        report["counts"]["errors"] += 1
        error_msg = f"Basel-Stadt law sync failed: {str(exc)}"
        report["errors"].append(error_msg)
        logging.error(error_msg)
        logging.error(traceback.format_exc())

    try:
        current_file_path = os.path.abspath(__file__)
        project_root = os.path.dirname(os.path.dirname(current_file_path))
        reports_dir = os.path.join(project_root, "reports")
        os.makedirs(reports_dir, exist_ok=True)
        timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
        report_file = os.path.join(reports_dir, f"law_bs_sync_report_{timestamp}.json")
        with open(report_file, "w", encoding="utf-8") as f:
            json.dump(report, f, indent=2, ensure_ascii=False)
        logging.info(f"Wrote LAW sync report: {report_file}")
    except Exception as report_error:
        logging.error(f"Failed to write LAW sync report: {str(report_error)}")

    logging.info(
        "LAW sync result: "
        f"{report['counts']['laws_created']} laws created, "
        f"{report['counts']['laws_updated']} laws updated, "
        f"{report['counts']['laws_unchanged']} laws unchanged, "
        f"{report['counts']['values_created']} literals created, "
        f"{report['counts']['values_updated']} literals updated, "
        f"{report['counts']['values_unchanged']} literals unchanged, "
        f"{report['counts']['errors']} errors"
    )
    return report


def main():
    sync_law_bs()


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
