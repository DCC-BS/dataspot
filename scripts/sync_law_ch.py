import datetime
import json
import logging
import os
import re
import traceback
import xml.etree.ElementTree as ET
from typing import Any, Dict, List, Optional

import config
from src.clients.law_client import LAWClient
from src.common import requests_get
from src.common import email_helpers


FEDLEX_SPARQL_ENDPOINT = "https://fedlex.data.admin.ch/sparqlendpoint"
WRITE_STATUS = "PUBLISHED"


def normalize_systematic_number(value: Any) -> str:
    if value is None:
        return ""
    normalized = str(value).strip()
    while len(normalized) >= 2 and normalized[0] == normalized[-1] and normalized[0] in ("'", '"'):
        normalized = normalized[1:-1].strip()
    return normalized


def _normalize_literal_field(value: Any) -> str:
    if value is None:
        return ""
    return (str(value) or "").strip().strip(" *")


def _normalize_whitespace(value: str) -> str:
    return " ".join((value or "").split()).strip()


def _element_text(element: Optional[ET.Element]) -> str:
    if element is None:
        return ""
    return _normalize_whitespace("".join(element.itertext()))


def _extract_title_from_fedlex_xml(root: ET.Element) -> str:
    title_candidates = []
    for tag_name in ("heading", "longTitle", "docTitle"):
        for node in root.findall(f".//{{*}}{tag_name}"):
            text = _element_text(node)
            if text:
                title_candidates.append(text)
    if title_candidates:
        return title_candidates[0]
    return ""


def _normalize_article_number(raw_number: str) -> str:
    value = _normalize_whitespace(raw_number)
    value = re.sub(r"^Art\.?\s*", "", value, flags=re.IGNORECASE)
    return _normalize_whitespace(value)


def parse_articles_from_fedlex_xml(xml_content: str) -> tuple[List[Dict[str, str]], str]:
    if not xml_content:
        return [], ""

    root = ET.fromstring(xml_content)
    extracted_title = _extract_title_from_fedlex_xml(root)

    articles: List[Dict[str, str]] = []
    seen_codes: set[str] = set()
    for article in root.findall(".//{*}article"):
        article_num = _normalize_article_number(_element_text(article.find("./{*}num")))
        if not article_num:
            continue

        pass

        code = f"Art. {article_num}"
        if code in seen_codes:
            continue

        short_text = _normalize_whitespace(_element_text(article.find("./{*}heading")))
        articles.append({"code": code, "shortText": short_text})
        seen_codes.add(code)

    return articles, extracted_title


def _binding_value(row: Dict[str, Any], key: str) -> str:
    field = row.get(key, {})
    if not isinstance(field, dict):
        return ""
    return (field.get("value") or "").strip()


def fetch_active_laws_from_fedlex(max_records: Optional[int] = None) -> List[Dict[str, str]]:
    query = """
        PREFIX jolux: <http://data.legilux.public.lu/resource/ontology/jolux#>
        PREFIX skos: <http://www.w3.org/2004/02/skos/core#>
        PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>

        SELECT DISTINCT
        (STR(?srNotation) AS ?rsNr)
        (STR(?dateApplicabilityNode) AS ?dateApplicability)
        ?title
        ?abrev
        ?fileUrl
        ?ccExpr
        WHERE {
        FILTER(?language = <http://publications.europa.eu/resource/authority/language/DEU>)

        ?consolidation a jolux:Consolidation .
        ?consolidation jolux:dateApplicability ?dateApplicabilityNode .
        OPTIONAL { ?consolidation jolux:dateEndApplicability ?dateEndApplicability }
        FILTER(
            xsd:date(?dateApplicabilityNode) <= xsd:date(now())
            && (!BOUND(?dateEndApplicability) || xsd:date(?dateEndApplicability) >= xsd:date(now()))
        )
        ?consolidation jolux:isRealizedBy ?consoExpr .
        ?consoExpr jolux:language ?language .
        ?consoExpr jolux:isEmbodiedBy ?consoManif .
        ?consoManif jolux:userFormat <https://fedlex.data.admin.ch/vocabulary/user-format/xml> .
        ?consoManif jolux:isExemplifiedBy ?fileUrl .
        ?consolidation jolux:isMemberOf ?cc .
        ?cc jolux:classifiedByTaxonomyEntry/skos:notation ?srNotation .
        OPTIONAL { ?cc jolux:dateNoLongerInForce ?ccNoLonger }
        OPTIONAL { ?cc jolux:dateEndApplicability ?ccEnd }
        FILTER(!BOUND(?ccNoLonger) || xsd:date(?ccNoLonger) > xsd:date(now()))
        FILTER(!BOUND(?ccEnd) || xsd:date(?ccEnd) >= xsd:date(now()))
        FILTER(datatype(?srNotation) = <https://fedlex.data.admin.ch/vocabulary/notation-type/id-systematique>)
        OPTIONAL {
            ?cc jolux:isRealizedBy ?ccExpr .
            ?ccExpr jolux:language ?language .
            ?ccExpr jolux:title ?title .
            OPTIONAL { ?ccExpr jolux:titleShort ?abrev }
        }
        }
        ORDER BY ?srNotation
        """
    response = requests_get(
        url=FEDLEX_SPARQL_ENDPOINT,
        params={"query": query, "format": "application/sparql-results+json"},
    )
    payload = response.json()
    rows = payload.get("results", {}).get("bindings", [])

    by_systematic_number: Dict[str, Dict[str, str]] = {}
    for row in rows:
        systematic_number = normalize_systematic_number(_binding_value(row, "rsNr"))
        if not systematic_number:
            continue

        record = {
            "systematic_number": systematic_number,
            "title_de": _binding_value(row, "title"),
            "date_applicability": _binding_value(row, "dateApplicability"),
            "expression": _binding_value(row, "ccExpr"),
            "consolidation": _binding_value(row, "consolidation"),
            "xml_url": _binding_value(row, "fileUrl"),
        }

        existing = by_systematic_number.get(systematic_number)
        if not existing or record["date_applicability"] > existing["date_applicability"]:
            by_systematic_number[systematic_number] = record

    records = sorted(by_systematic_number.values(), key=lambda r: r["systematic_number"])
    total_records = len(records)
    if max_records is not None:
        records = records[:max_records]
        logging.info(
            f"Retrieved {total_records} active SR laws from Fedlex SPARQL and kept {len(records)} due to max_records={max_records}"
        )
    else:
        logging.info(f"Retrieved {total_records} active SR laws from Fedlex SPARQL")
    return records


def build_law_cache(
    assets: List[Dict[str, Any]], law_collection_label: str
) -> Dict[str, Dict[str, Any]]:
    by_law_label: Dict[str, Dict[str, Any]] = {}
    by_systematic_number: Dict[str, Dict[str, Any]] = {}

    for asset in assets:
        if asset.get("_type") != "ReferenceObject":
            continue
        if asset.get("inCollection") != law_collection_label:
            continue

        systematic_number = normalize_systematic_number(asset["systematic_number"])
        if not systematic_number:
            raise ValueError(
                f"LAW asset {asset.get('id')} has empty systematic_number after normalization"
            )

        law_entry = {
            "id": asset.get("id"),
            "label": asset.get("label", ""),
            "description": asset.get("description", ""),
            "title": asset.get("title", ""),
            "legal_form": asset.get("legal_form", ""),
            "systematic_number": systematic_number,
            "values_by_code": {},
        }
        label = asset["label"]
        by_law_label[label] = law_entry
        by_systematic_number[systematic_number] = law_entry

    mapped_literals = 0
    for asset in assets:
        if asset.get("_type") != "ReferenceValue":
            continue
        parent_label_raw = asset.get("literalOf")
        if parent_label_raw is None:
            raise ValueError(
                f"ReferenceValue id={asset.get('id')} code={asset.get('code')} has no literalOf; "
                "every literal must reference a parent ReferenceObject"
            )
        parent_label_normalized = normalize_systematic_number(parent_label_raw)
        parent_label_lookup = parent_label_raw
        if parent_label_lookup not in by_law_label and parent_label_normalized in by_law_label:
            parent_label_lookup = parent_label_normalized

        if parent_label_lookup not in by_law_label:
            raise ValueError(
                f"ReferenceValue id={asset.get('id')} code={asset.get('code')} literalOf='{parent_label_raw}' "
                "does not match any LAW ReferenceObject in the target collection; breaking data integrity"
            )

        time_series = asset["timeSeries"]
        if len(time_series) > 1:
            raise ValueError("The code currently does not support multiple entries in the time series.")
        ts0 = time_series[0]
        code = _normalize_literal_field(ts0["code"])
        short_text = _normalize_literal_field(ts0.get("shortText"))

        by_law_label[parent_label_lookup]["values_by_code"][code] = {
            "id": asset.get("id"),
            "code": code,
            "shortText": short_text,
        }
        mapped_literals += 1

    logging.info(
        f"Built LAW cache with {len(by_systematic_number)} ReferenceObjects and {mapped_literals} literals from Download API"
    )
    return by_systematic_number


def build_reference_object_payload(
    systematic_number: str,
    title_de: str,
    original_url_de: str,
    legal_form: str = "",
) -> Dict[str, Any]:
    return {
        "_type": "ReferenceObject",
        "label": f"SR {systematic_number} - {title_de}",
        "description": original_url_de or "",
        "title": "",
        "customProperties": {
            "legal_form": legal_form or "",
            "systematic_number": systematic_number,
        },
    }


def build_reference_value_payload(code: str, short_text: str) -> Dict[str, Any]:
    if not short_text:
        short_text = ""

    return {
        "_type": "ReferenceValue",
        "timeSeries": [
            {
                "code": code,
                "shortText": short_text,
                "validFrom": -2208988800000,
                "validTo": 32503593600000,
            }
        ],
    }


def _create_law_email_content(report: Dict[str, Any]) -> tuple:
    counts = report.get("counts", {})
    marked = counts.get("values_marked_for_deletion", 0) + counts.get("laws_marked_for_deletion", 0)
    errors = counts.get("errors", 0)
    if marked == 0 and errors == 0:
        return None, None, False

    is_error = report.get("status") == "error"
    if is_error:
        email_subject = f"[ERROR][{config.database_name}/GS] LAW CH Sync: failed"
    else:
        email_subject = (
            f"[{config.database_name}/GS] LAW CH Sync: " f"{marked} marked for deletion, {errors} errors"
        )

    email_text = "Hi there,\n\n"
    if is_error:
        email_text += "There was an error during the Swiss SR law sync in Dataspot.\n"
        for err in report.get("errors", [])[:10]:
            email_text += f"- {err}\n"
        if len(report.get("errors", [])) > 10:
            email_text += f"- ... and {len(report['errors']) - 10} more (see attachment)\n"
        email_text += "\n"
    else:
        email_text += "The Swiss SR law sync completed. The following assets were marked for deletion (still in use):\n\n"
        for item in report.get("marked_items", []):
            link = item.get("link", "")
            if item.get("type") == "ReferenceValue":
                email_text += f"- ReferenceValue code={item.get('code', '')}: {link}\n"
            else:
                email_text += f"- ReferenceObject {item.get('label', '')}: {link}\n"
        if errors > 0:
            email_text += f"\nAdditionally, {errors} error(s) occurred (see report attachment).\n"
        email_text += "\n"

    email_text += "Best regards,\n"
    email_text += "Your Dataspot LAW Sync Assistant\n"
    return email_subject, email_text, True


def sync_law_ch() -> Dict[str, Any]:
    logging.info("Starting Swiss SR law sync")

    report = {
        "status": "pending",
        "counts": {
            "laws_created": 0,
            "laws_updated": 0,
            "laws_unchanged": 0,
            "values_created": 0,
            "values_updated": 0,
            "values_unchanged": 0,
            "values_deleted": 0,
            "values_marked_for_deletion": 0,
            "laws_deleted": 0,
            "laws_marked_for_deletion": 0,
            "errors": 0,
        },
        "errors": [],
        "marked_items": [],
    }

    law_client = LAWClient()
    try:
        fedlex_laws = fetch_active_laws_from_fedlex(max_records=1)
        law_collection_uuid = law_client.resolve_collection_uuid_by_label(
            config.law_ch_collection_label
        )
        logging.info(f"Resolved LAW CH target collection UUID: {law_collection_uuid}")
        scheme_assets = law_client.download_law_assets_in_collection(collection_uuid=law_collection_uuid)

        law_cache = build_law_cache(assets=scheme_assets, law_collection_label=config.law_ch_collection_label)

        total = len(fedlex_laws)
        for idx, record in enumerate(fedlex_laws, start=1):
            systematic_number = normalize_systematic_number(record.get("systematic_number"))
            title_de = (record.get("title_de") or "").strip()
            xml_url = (record.get("xml_url") or "").strip()
            expression_url = (record.get("expression") or "").strip()
            legal_form = ""
            original_url_de = expression_url.replace("https://fedlex.data.admin.ch", "https://www.fedlex.admin.ch")

            if not systematic_number or not xml_url:
                report["counts"]["errors"] += 1
                error_msg = (
                    f"Skipping record due to missing required field(s): "
                    f"systematic_number='{systematic_number}', xml_url='{xml_url}'"
                )
                report["errors"].append(error_msg)
                logging.error(error_msg)
                continue

            xml_response = requests_get(url=xml_url)
            paragraphs, parsed_title = parse_articles_from_fedlex_xml(xml_response.text)
            if not title_de and parsed_title:
                title_de = parsed_title

            if not title_de:
                report["counts"]["errors"] += 1
                error_msg = f"Skipping record systematic_number={systematic_number} due to missing title"
                report["errors"].append(error_msg)
                logging.error(error_msg)
                continue

            desired_law = build_reference_object_payload(
                systematic_number=systematic_number,
                title_de=title_de,
                original_url_de=original_url_de,
                legal_form=legal_form,
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
                    f"[{idx}/{total}] Created CH law '{desired_law['label']}' with systematic_number={systematic_number}"
                )
                if current_law_id:
                    deployment_ok = law_client.create_reference_object_deployment(
                        law_id=current_law_id,
                        systematic_number=systematic_number,
                        system_uuid=config.law_ch_system_uuid,
                    )
                    if not deployment_ok:
                        report["counts"]["errors"] += 1
                        report["errors"].append(
                            f"Failed to create CH system deployment for law systematic_number={systematic_number} law_id={current_law_id}"
                        )
            else:
                current_law_id = existing_law.get("id")
                current_values_by_code = existing_law.get("values_by_code", {})

                law_changed = (
                    existing_law.get("label") != desired_law["label"]
                    or (existing_law.get("description") or "") != desired_law["description"]
                    or (existing_law.get("title") or "") != (desired_law.get("title") or "")
                    or (existing_law.get("legal_form") or "")
                    != (desired_law.get("customProperties", {}).get("legal_form") or "")
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
                        f"Updated CH law '{desired_law['label']}' with systematic_number={systematic_number}"
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

            if not paragraphs:
                paragraphs.append({"code": "§", "shortText": "(keine Rechtsnormen)"})

            for paragraph in paragraphs:
                desired_value_code = _normalize_literal_field(paragraph["code"])
                desired_value_short_text = _normalize_literal_field(paragraph["shortText"])

                desired_value = build_reference_value_payload(
                    code=desired_value_code,
                    short_text=desired_value_short_text,
                )
                existing_value = current_values_by_code.get(desired_value_code)

                if not existing_value:
                    law_client.create_reference_value(
                        law_id=current_law_id, data=desired_value, status=WRITE_STATUS
                    )
                    report["counts"]["values_created"] += 1
                    logging.info(
                        f"Created CH literal code={desired_value_code} for law systematic_number={systematic_number}"
                    )
                    continue

                value_changed = (
                    (existing_value.get("code") or "") != desired_value_code
                    or (existing_value.get("shortText") or "") != desired_value_short_text
                )

                if value_changed:
                    law_client.update_reference_value(
                        value_id=existing_value.get("id"),
                        data=desired_value,
                        status=WRITE_STATUS,
                    )
                    report["counts"]["values_updated"] += 1
                    logging.info(
                        f"Updated CH literal code={desired_value_code} for law systematic_number={systematic_number}"
                    )
                else:
                    report["counts"]["values_unchanged"] += 1

            desired_value_codes = {_normalize_literal_field(p["code"]) for p in paragraphs}
            obsolete_codes = set(current_values_by_code.keys()) - desired_value_codes
            if obsolete_codes:
                child_in_use = law_client.get_child_literal_ids_in_use(current_law_id)
                for code in sorted(obsolete_codes):
                    literal_info = current_values_by_code[code]
                    literal_id = literal_info.get("id")
                    if not literal_id:
                        report["counts"]["errors"] += 1
                        report["errors"].append(
                            f"Obsolete literal code={code} has no id for systematic_number={systematic_number}"
                        )
                        logging.error(
                            f"Obsolete literal code={code} has no id for systematic_number={systematic_number}"
                        )
                        continue
                    try:
                        if literal_id in child_in_use:
                            law_client.mark_literal_for_deletion(literal_id)
                            report["counts"]["values_marked_for_deletion"] += 1
                            report["marked_items"].append(
                                {
                                    "type": "ReferenceValue",
                                    "id": literal_id,
                                    "code": code,
                                    "link": f"{config.base_url}/web/{config.database_name}/literals/{literal_id}",
                                }
                            )
                            logging.info(
                                f"Marked CH literal code={code} for deletion (in use) for law systematic_number={systematic_number}"
                            )
                        else:
                            law_client.delete_literal(literal_id)
                            report["counts"]["values_deleted"] += 1
                            logging.info(
                                f"Deleted CH literal code={code} for law systematic_number={systematic_number}"
                            )
                    except Exception as exc:
                        report["counts"]["errors"] += 1
                        report["errors"].append(
                            f"Failed to process obsolete literal code={code} id={literal_id}: {str(exc)}"
                        )
                        logging.error(
                            f"Failed to process obsolete literal code={code} id={literal_id}: {str(exc)}"
                        )

        fedlex_systematic_numbers = {
            normalize_systematic_number(r.get("systematic_number")) for r in fedlex_laws
        }
        obsolete_systematic_numbers = set(law_cache.keys()) - fedlex_systematic_numbers
        for systematic_number in obsolete_systematic_numbers:
            existing_law = law_cache.get(systematic_number)
            if not existing_law:
                continue
            enum_id = existing_law.get("id")
            if not enum_id:
                report["counts"]["errors"] += 1
                report["errors"].append(f"Obsolete law systematic_number={systematic_number} has no id")
                logging.error(f"Obsolete law systematic_number={systematic_number} has no id")
                continue
            parent_in_use = law_client.is_parent_in_use(enum_id)
            child_in_use = law_client.get_child_literal_ids_in_use(enum_id)
            values_by_code = existing_law.get("values_by_code", {})
            children_sorted = sorted(values_by_code.items(), key=lambda x: x[0])
            any_child_marked = False
            for code, literal_info in children_sorted:
                literal_id = literal_info.get("id")
                if not literal_id:
                    report["counts"]["errors"] += 1
                    report["errors"].append(
                        f"Obsolete law literal code={code} has no id for systematic_number={systematic_number}"
                    )
                    logging.error(
                        f"Obsolete law literal code={code} has no id for systematic_number={systematic_number}"
                    )
                    continue
                try:
                    if literal_id in child_in_use:
                        law_client.mark_literal_for_deletion(literal_id)
                        report["counts"]["values_marked_for_deletion"] += 1
                        any_child_marked = True
                        report["marked_items"].append(
                            {
                                "type": "ReferenceValue",
                                "id": literal_id,
                                "code": code,
                                "link": f"{config.base_url}/web/{config.database_name}/literals/{literal_id}",
                            }
                        )
                        logging.info(
                            f"Marked CH literal code={code} for deletion (in use) for obsolete law systematic_number={systematic_number}"
                        )
                    else:
                        law_client.delete_literal(literal_id)
                        report["counts"]["values_deleted"] += 1
                        logging.info(
                            f"Deleted CH literal code={code} for obsolete law systematic_number={systematic_number}"
                        )
                except Exception as exc:
                    report["counts"]["errors"] += 1
                    report["errors"].append(
                        f"Failed to process obsolete law literal code={code} id={literal_id}: {str(exc)}"
                    )
                    logging.error(
                        f"Failed to process obsolete law literal code={code} id={literal_id}: {str(exc)}"
                    )
            try:
                if parent_in_use or any_child_marked:
                    law_client.mark_reference_object_for_deletion(enum_id, status="REVIEWDCC2")
                    report["counts"]["laws_marked_for_deletion"] += 1
                    if parent_in_use:
                        report["marked_items"].append(
                            {
                                "type": "ReferenceObject",
                                "id": enum_id,
                                "label": existing_law.get("label", ""),
                                "link": f"{config.base_url}/web/{config.database_name}/enumerations/{enum_id}",
                            }
                        )
                        logging.info(
                            f"Marked CH law for deletion (in use) systematic_number={systematic_number}"
                        )
                    else:
                        logging.info(
                            f"Marked CH law for deletion (child in use) systematic_number={systematic_number}"
                        )
                else:
                    law_client.delete_reference_object(enum_id)
                    report["counts"]["laws_deleted"] += 1
                    logging.info(f"Deleted obsolete CH law systematic_number={systematic_number}")
            except Exception as exc:
                report["counts"]["errors"] += 1
                report["errors"].append(
                    f"Failed to process obsolete law systematic_number={systematic_number} id={enum_id}: {str(exc)}"
                )
                logging.error(
                    f"Failed to process obsolete law systematic_number={systematic_number} id={enum_id}: {str(exc)}"
                )

        report["status"] = "success"
    except Exception as exc:
        report["status"] = "error"
        report["counts"]["errors"] += 1
        error_msg = f"Swiss SR law sync failed: {str(exc)}"
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
        report_file = os.path.join(reports_dir, f"law_ch_sync_report_{timestamp}.json")
        with open(report_file, "w", encoding="utf-8") as handle:
            json.dump(report, handle, indent=2, ensure_ascii=False)
        logging.info(f"Wrote LAW CH sync report: {report_file}")
    except Exception as report_error:
        logging.error(f"Failed to write LAW CH sync report: {str(report_error)}")

    email_subject, email_content, should_send = _create_law_email_content(report)
    if should_send:
        try:
            attachment = report_file if report_file and os.path.exists(report_file) else None
            msg = email_helpers.create_email_msg(
                subject=email_subject,
                text=email_content,
                attachment=attachment,
            )
            email_helpers.send_email(msg, technical_only=True)
            logging.info("LAW CH sync email notification sent successfully")
        except Exception as email_error:
            logging.error(f"Failed to send LAW CH sync email notification: {str(email_error)}")
    else:
        logging.info("No marks-for-deletion or errors - email notification not sent")

    logging.info(
        "LAW CH sync result: "
        f"{report['counts']['laws_created']} laws created, "
        f"{report['counts']['laws_updated']} laws updated, "
        f"{report['counts']['laws_unchanged']} laws unchanged, "
        f"{report['counts']['values_created']} literals created, "
        f"{report['counts']['values_updated']} literals updated, "
        f"{report['counts']['values_unchanged']} literals unchanged, "
        f"{report['counts']['values_deleted']} literals deleted, "
        f"{report['counts']['values_marked_for_deletion']} literals marked for deletion, "
        f"{report['counts']['laws_deleted']} laws deleted, "
        f"{report['counts']['laws_marked_for_deletion']} laws marked for deletion, "
        f"{report['counts']['errors']} errors"
    )
    return report


def main():
    sync_law_ch()


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
