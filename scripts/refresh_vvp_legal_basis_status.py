import datetime
import json
import logging
import os
import traceback
from typing import Any, Dict, List, Optional

import config
from src.clients.helpers import coerce_utc_midnight_ms, strip_quotes
from src.clients.law_client import LAWClient
from src.clients.vvp_client import VVPClient
from src.common import email_helpers


QUALIFIER_PROPERTY = "qualifier"
CURRENT_AS_OF_PROPERTY = "currentAsOf"

STATUS_UNKNOWN = "unknown"
STATUS_CHANGED = "changed"
STATUS_REPEALED = "repealed"
STATUS_CURRENT = "current"

PUBLISHED_STATUS = "PUBLISHED"


def _normalize_id(value: Any) -> str:
    if value is None:
        return ""
    return str(value).strip()


def _normalize_literal_status(value: Any) -> Optional[str]:
    """Normalize a Usage qualifier value read via the Query API."""
    if value is None:
        return None
    text = strip_quotes(str(value))
    text = (text or "").strip()
    return text or None


def build_law_target_index(assets: List[Dict[str, Any]]) -> Dict[str, Dict[str, Any]]:
    """
    Build an id -> {"status": str, "version_active_since": Optional[int]} map covering every
    ReferenceObject and ReferenceValue in the Gesetzessammlungen scheme.

    Custom properties are flat on Download API assets (e.g. version_active_since at top level).
    ReferenceValue entries inherit version_active_since from their parent ReferenceObject, looked
    up via literalOf (a label, not a UUID, on Download API assets).
    """
    objects_by_label: Dict[str, Dict[str, Any]] = {}
    index: Dict[str, Dict[str, Any]] = {}

    for asset in assets:
        if asset.get("_type") != "ReferenceObject":
            continue
        asset_id = _normalize_id(asset.get("id"))
        if not asset_id:
            continue
        entry = {
            "status": str(asset.get("status") or "").strip(),
            "version_active_since": coerce_utc_midnight_ms(asset.get("version_active_since")),
        }
        index[asset_id] = entry
        label = asset.get("label")
        if label:
            objects_by_label[label] = entry

    mapped_values = 0
    skipped_values = 0
    for asset in assets:
        if asset.get("_type") != "ReferenceValue":
            continue
        asset_id = _normalize_id(asset.get("id"))
        if not asset_id:
            continue
        parent_label = asset.get("literalOf")
        parent_entry = objects_by_label.get(parent_label) if parent_label else None
        if parent_entry is None:
            skipped_values += 1
            continue
        index[asset_id] = {
            "status": str(asset.get("status") or "").strip(),
            "version_active_since": parent_entry["version_active_since"],
        }
        mapped_values += 1

    logging.info(
        "Built LAW target index with %s ReferenceObjects and %s ReferenceValues "
        "(%s values skipped, no matching parent) from Download API",
        len(objects_by_label),
        mapped_values,
        skipped_values,
    )
    return index


def fetch_personendaten_law_usages(vvp_client: VVPClient) -> List[Dict[str, Any]]:
    """
    Load all Usage rows for PUBLISHED Processings in the Personendaten scheme,
    together with the Processing's currentAsOf and the Usage's current qualifier.
    """
    query = f"""
        WITH processing_custom_props AS (
            SELECT cp.resource_id,
                   MAX(CASE WHEN cp.name = '{CURRENT_AS_OF_PROPERTY}' THEN cp.value END) AS current_as_of
            FROM dataspot.customproperties_view cp
            WHERE cp.name = '{CURRENT_AS_OF_PROPERTY}'
            GROUP BY cp.resource_id
        )
        SELECT
            u.id AS usage_id,
            u.usage_of,
            u.qualifier,
            p.id AS processing_id,
            p.label AS processing_label,
            props.current_as_of
        FROM dataspot.usageof_view u
        JOIN dataspot.processing_view p
          ON p.id = u.resource_id
         AND p.model_id = u.model_id
        LEFT JOIN processing_custom_props props ON props.resource_id = p.id
        WHERE p.status = '{PUBLISHED_STATUS}'
    """
    rows = vvp_client.execute_query_api(sql_query=query)
    logging.info("Loaded %s Personendaten Usage rows for legal-basis status refresh", len(rows))
    return rows


def compute_legal_basis_status(
    target_status: str,
    version_active_since_ms: Optional[int],
    current_as_of_ms: int,
) -> str:
    """
    Decide the qualifier literal for a Personendaten Usage that links to a
    Gesetzessammlungen ReferenceObject or ReferenceValue.

    Callers must only invoke this once the Usage's usageOf has been confirmed to be in the
    Gesetzessammlungen target index, and current_as_of_ms must already be resolved (currentAsOf
    is mandatory on Processing; a missing value is a data error to be handled by the caller,
    not passed into this function).
    """
    if target_status != PUBLISHED_STATUS:
        return STATUS_REPEALED

    if version_active_since_ms is None:
        return STATUS_UNKNOWN

    if version_active_since_ms > current_as_of_ms:
        return STATUS_CHANGED

    return STATUS_CURRENT


def _create_legal_basis_status_email_content(report: Dict[str, Any]) -> tuple:
    """
    Create email content for the legal-basis status refresh. Send only if errors occurred.
    Returns (email_subject, email_text, should_send).
    """
    counts = report.get("counts", {})
    errors = counts.get("errors", 0)
    if errors == 0:
        return None, None, False

    is_error = report.get("status") == "error"
    if is_error:
        email_subject = f"[ERROR][{config.database_name}/VVP] Legal-Basis-Status Refresh: failed"
    else:
        email_subject = f"[{config.database_name}/VVP] Legal-Basis-Status Refresh: {errors} errors"

    email_text = "Hi there,\n\n"
    if is_error:
        email_text += "There was an error during the Personendaten legal-basis status refresh in Dataspot.\n"
    else:
        email_text += "The Personendaten legal-basis status refresh completed with errors:\n"
    for err in report.get("errors", [])[:10]:
        email_text += f"- {err}\n"
    if len(report.get("errors", [])) > 10:
        email_text += f"- ... and {len(report['errors']) - 10} more (see attachment)\n"
    email_text += "\n"
    email_text += "Best regards,\n"
    email_text += "Your Dataspot VVP Legal-Basis-Status Refresh Assistant\n"
    return email_subject, email_text, True


def refresh_vvp_legal_basis_status() -> Dict[str, Any]:
    logging.info("Starting Personendaten legal-basis status refresh")

    report = {
        "status": "pending",
        "counts": {
            "updated": 0,
            "unchanged": 0,
            "skipped_other": 0,
            "errors": 0,
        },
        "errors": [],
    }

    law_client = LAWClient()
    vvp_client = VVPClient()

    try:
        law_assets = law_client.download_all_law_assets()
        law_target_index = build_law_target_index(law_assets)

        usage_rows = fetch_personendaten_law_usages(vvp_client)

        for row in usage_rows:
            usage_id = _normalize_id(row.get("usage_id"))
            usage_of = _normalize_id(row.get("usage_of"))
            processing_id = _normalize_id(row.get("processing_id"))
            processing_label = str(row.get("processing_label") or "").strip()

            if not usage_id or not usage_of:
                report["counts"]["errors"] += 1
                error_msg = f"Usage row missing id or usageOf: usage_id={usage_id}, usage_of={usage_of}"
                report["errors"].append(error_msg)
                logging.error(error_msg)
                continue

            target = law_target_index.get(usage_of)
            if target is None:
                report["counts"]["skipped_other"] += 1
                continue

            current_status = _normalize_literal_status(row.get(QUALIFIER_PROPERTY))

            current_as_of_ms = coerce_utc_midnight_ms(row.get("current_as_of"))
            if current_as_of_ms is None:
                report["counts"]["errors"] += 1
                error_msg = (
                    f"Processing id={processing_id} label='{processing_label}' has no currentAsOf; "
                    f"skipping usage id={usage_id}"
                )
                report["errors"].append(error_msg)
                logging.error(error_msg)
                continue

            new_status = compute_legal_basis_status(
                target_status=target["status"],
                version_active_since_ms=target["version_active_since"],
                current_as_of_ms=current_as_of_ms,
            )

            if new_status == current_status:
                report["counts"]["unchanged"] += 1
                continue

            try:
                vvp_client.patch_usage(
                    usage_uuid=usage_id,
                    payload={QUALIFIER_PROPERTY: new_status},
                )
                report["counts"]["updated"] += 1
                logging.info(
                    "Updated qualifier usage_id=%s %s -> %s",
                    usage_id,
                    current_status or "(empty)",
                    new_status,
                )
            except Exception as exc:
                report["counts"]["errors"] += 1
                error_msg = f"Failed to patch usage id={usage_id} to status={new_status}: {str(exc)}"
                report["errors"].append(error_msg)
                logging.error(error_msg)

        report["status"] = "success"
    except Exception as exc:
        report["status"] = "error"
        report["counts"]["errors"] += 1
        error_msg = f"Personendaten legal-basis status refresh failed: {str(exc)}"
        report["errors"].append(error_msg)
        logging.error(error_msg)
        logging.error(traceback.format_exc())

    report_file = None
    try:
        current_file_path = os.path.abspath(__file__)
        project_root = os.path.dirname(os.path.dirname(current_file_path))
        reports_dir = os.path.join(project_root, "reports")
        os.makedirs(reports_dir, exist_ok=True)
        timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
        report_file = os.path.join(reports_dir, f"vvp_legal_basis_status_refresh_report_{timestamp}.json")
        with open(report_file, "w", encoding="utf-8") as f:
            json.dump(report, f, indent=2, ensure_ascii=False)
        logging.info(f"Wrote VVP legal-basis status refresh report: {report_file}")
    except Exception as report_error:
        logging.error(f"Failed to write VVP legal-basis status refresh report: {str(report_error)}")

    email_subject, email_content, should_send = _create_legal_basis_status_email_content(report)
    if should_send:
        try:
            attachment = report_file if report_file and os.path.exists(report_file) else None
            msg = email_helpers.create_email_msg(
                subject=email_subject,
                text=email_content,
                attachment=attachment,
            )
            email_helpers.send_email(msg, technical_only=True)
            logging.info("VVP legal-basis status refresh email notification sent successfully")
        except Exception as email_error:
            logging.error(f"Failed to send VVP legal-basis status refresh email notification: {str(email_error)}")
    else:
        logging.info("No errors - email notification not sent")

    logging.info(
        "VVP legal-basis status refresh result: "
        f"{report['counts']['updated']} updated, "
        f"{report['counts']['unchanged']} unchanged, "
        f"{report['counts']['skipped_other']} skipped (not a law usage), "
        f"{report['counts']['errors']} errors"
    )
    return report


def main():
    refresh_vvp_legal_basis_status()


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
