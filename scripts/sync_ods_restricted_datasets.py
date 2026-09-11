import logging
import json
import os
import datetime
import time
import traceback

import config
from src.clients.dnk_client import DNKClient
from src.clients.helpers import url_join
from src.common import email_helpers as email_helpers
import ods_utils_py as ods_utils
from src.dataset_transformer import LICENSE_MAP, _get_field_value, transform_ods_to_dnk
from src.ods_client import ODSClient


def main():
    sync_ods_restricted_datasets()


def sync_ods_restricted_datasets(max_datasets: int = None, batch_size: int = 50):
    """
    Synchronize restricted (unpublished) ODS datasets with Dataspot using DNKClient.

    This method:
    1. Creates a DNKClient and ODSClient instance
    2. Ensures the "(unveröffentlicht)" and "(intern)" sibling collections exist
    3. Retrieves the full ODS dataset listing (restricted and unrestricted) from ODS
    4. For each restricted dataset that is new, or already exists with status WORKING,
       retrieves metadata and transforms it into a DRAFT (WORKING) dataset without
       compositions, Huwise deployment, or OGD distributions
    5. Syncs the batch into the "(unveröffentlicht)" collection
    6. Marks datasets no longer present in ODS at all for deletion review
    7. Provides a summary of changes and logs a detailed report
    8. Sends an email notification if there were changes

    Args:
        max_datasets (int, optional): Maximum number of ODS datasets to process. Defaults to None (all datasets).
        batch_size (int, optional): Number of datasets to process in each sync batch. Defaults to 50.
    """
    logging.info("Starting ODS restricted datasets synchronization...")

    # Initialize clients
    dataspot_client = DNKClient()
    ods_client = ODSClient()

    # Initialize variables
    total_processed = 0
    processed_ids = []
    batch = []
    report_filename = None

    # Store sync results for reporting
    sync_results = {
        'status': 'pending',
        'message': '',
        'counts': {
            'total': 0,
            'created': 0,
            'updated': 0,
            'unchanged': 0,
            'deleted': 0,
            'errors': 0,
            'processed': 0,
            'skipped_unrestricted': 0,
            'skipped_not_working': 0,
        },
        'details': {
            'creations': {'count': 0, 'items': []},
            'updates': {'count': 0, 'items': []},
            'deletions': {'count': 0, 'items': []},
            'errors': {'count': 0, 'items': []},
        }
    }

    try:
        # Ensure the sibling collections exist
        logging.info("Step 1: Ensuring restricted/internal sibling collections exist...")
        dataspot_client.ensure_collection_exists(
            config.dnk_restricted_ods_imports_collection_name,
            config.dnk_restricted_ods_imports_collection_path,
        )
        dataspot_client.ensure_collection_exists(
            config.dnk_internal_ods_imports_collection_name,
            config.dnk_internal_ods_imports_collection_path,
        )
        restricted_collection_path = url_join(
            *config.dnk_restricted_ods_imports_collection_path,
            config.dnk_restricted_ods_imports_collection_name,
        )

        # Get the full ODS dataset listing (restricted and unrestricted)
        logging.info(f"Step 2: Retrieving {max_datasets or 'all'} dataset ids (with restricted flag) from ODS...")
        ods_datasets = ods_client.get_all_dataset_ids_with_restricted_flag(max_datasets=max_datasets)
        logging.info(f"Found {len(ods_datasets)} datasets in ODS")
        full_ods_dataset_ids = {entry['dataset_id'] for entry in ods_datasets}

        # Get all existing Dataspot OGD datasets, keyed by odsDataportalId
        all_dataspot_datasets = dataspot_client.get_datasets_with_cache()

        # Process datasets
        logging.info("Step 3: Processing restricted datasets - downloading metadata and transforming...")

        for idx, entry in enumerate(ods_datasets):
            ods_id = entry['dataset_id']
            is_restricted = entry['is_restricted']

            if not is_restricted:
                logging.info(f"Skipping unrestricted dataset {ods_id} (handled by sync_ods_datasets.py)")
                sync_results['counts']['skipped_unrestricted'] += 1
                continue

            existing_entry = all_dataspot_datasets.get(ods_id)
            if existing_entry:
                existing_status = existing_entry.get('status')
                if existing_status != 'WORKING':
                    logging.info(f"Skipping restricted dataset {ods_id}: existing status is '{existing_status}' (not WORKING)")
                    sync_results['counts']['skipped_not_working'] += 1
                    continue

            logging.info(f"[{idx+1}/{len(ods_datasets)}] Processing restricted dataset {ods_id}...")

            # Get metadata from ODS and transform to Dataspot dataset
            ods_metadata_from_automation_api = ods_utils.get_dataset_metadata(dataset_id=ods_id)
            # The following is a dirty quick solution so that it works.
            # Will break as soon as the Explore API V2.1 is deprecated, since the entire url is hardcoded.
            ods_metadata_from_explore_api_response = ods_utils.requests_get(url=f"https://data.bs.ch/api/explore/v2.1/catalog/datasets/{ods_id}")
            ods_metadata_from_explore_api = ods_metadata_from_explore_api_response.json()

            license_id = None
            if 'internal' in ods_metadata_from_automation_api and 'license_id' in ods_metadata_from_automation_api['internal']:
                license_id = _get_field_value(ods_metadata_from_automation_api['internal']['license_id'])
            if license_id and license_id not in LICENSE_MAP:
                error_msg = f"Unknown license ID: {license_id}"
                logging.error(f"Skipping restricted dataset {ods_id} due to faulty license: {error_msg}")
                sync_results['counts']['errors'] += 1
                sync_results['details']['errors']['count'] += 1
                sync_results['details']['errors']['items'].append({
                    "ods_id": ods_id,
                    "message": error_msg
                })
                continue

            dataset = transform_ods_to_dnk(ods_metadata_from_automation_api=ods_metadata_from_automation_api,
                                           ods_metadata_from_explore_api=ods_metadata_from_explore_api,
                                           ods_dataset_id=ods_id,
                                           is_restricted=True)

            batch.append(dataset)
            processed_ids.append(ods_id)
            total_processed += 1

            logging.info(f"Successfully transformed restricted dataset {ods_id}: {dataset.name}")
            logging.info("Waiting 1 second to not overload Huwise")
            time.sleep(1)

            # Process in smaller batches to avoid memory issues
            if len(batch) >= batch_size or idx == len(ods_datasets) - 1:
                if batch:
                    batch_num = len(batch)
                    logging.info(f"Step 4: Syncing batch of {batch_num} restricted datasets...")

                    sync_summary = dataspot_client.dataset_handler.sync_datasets(
                        datasets=batch,
                        status="WORKING",
                        ensure_deployments=False,
                        ensure_distributions=False,
                        in_collection_path=restricted_collection_path,
                        mapping_in_collection=config.dnk_restricted_ods_imports_collection_name,
                    )

                    logging.info(f"Batch sync completed. Response summary: {sync_summary}")

                    # Update overall counts
                    sync_results['counts']['created'] += sync_summary.get('created', 0)
                    sync_results['counts']['updated'] += sync_summary.get('updated', 0)
                    sync_results['counts']['errors'] += sync_summary.get('errors', 0)
                    sync_results['counts']['unchanged'] += sync_summary.get('unchanged', 0)
                    sync_results['counts']['total'] += (
                        sync_summary.get('created', 0) +
                        sync_summary.get('updated', 0)
                    )

                    # Append detailed change information to the report
                    if 'details' in sync_summary:
                        if 'creations' in sync_summary['details']:
                            sync_results['details']['creations']['count'] += sync_summary['details']['creations'].get('count', 0)
                            sync_results['details']['creations']['items'].extend(sync_summary['details']['creations'].get('items', []))

                        if 'updates' in sync_summary['details']:
                            sync_results['details']['updates']['count'] += sync_summary['details']['updates'].get('count', 0)
                            sync_results['details']['updates']['items'].extend(sync_summary['details']['updates'].get('items', []))

                        if 'errors' in sync_summary['details']:
                            sync_results['details']['errors']['count'] += sync_summary['details']['errors'].get('count', 0)
                            sync_results['details']['errors']['items'].extend(sync_summary['details']['errors'].get('items', []))

                    # Clear the batch for the next iteration
                    batch = []

        # After all batches have been processed, handle deletions
        logging.info("Step 5: Processing deletions - identifying restricted datasets no longer in ODS...")

        datasets_to_delete = [
            ods_id for ods_id, info in all_dataspot_datasets.items()
            if info.get('status') == 'WORKING' and ods_id not in full_ods_dataset_ids
        ]

        if datasets_to_delete:
            logging.info(f"Found {len(datasets_to_delete)} restricted datasets to mark for deletion")

            for ods_id in datasets_to_delete:
                try:
                    deleted = dataspot_client.dataset_handler.delete_dataset(ods_id, fail_if_not_exists=False)

                    if deleted:
                        sync_results['counts']['deleted'] += 1
                        sync_results['counts']['total'] += 1
                        sync_results['details']['deletions']['count'] += 1

                        dataset_info = all_dataspot_datasets.get(ods_id)
                        title = dataset_info.get('label', f"<Unnamed Dataset {ods_id}>") if dataset_info else f"<Unnamed Dataset {ods_id}>"
                        uuid = dataset_info.get('id') if dataset_info else None

                        dataspot_link = f"{config.base_url}/web/{config.database_name}/datasets/{uuid}" if uuid else ''

                        sync_results['details']['deletions']['items'].append({
                            "ods_id": ods_id,
                            "title": title,
                            "uuid": uuid,
                            "link": dataspot_link
                        })
                        logging.info(f"Marked restricted dataset with odsDataportalId {ods_id} for deletion: {title} (Link: {dataspot_link})")

                except Exception as e:
                    error_msg = f"Error marking restricted dataset with odsDataportalId {ods_id} for deletion: {str(e)}"
                    logging.error(error_msg)

                    sync_results['counts']['errors'] += 1
                    sync_results['details']['errors']['count'] += 1
                    sync_results['details']['errors']['items'].append({
                        "ods_id": ods_id,
                        "message": error_msg
                    })
        else:
            logging.info("No restricted datasets found for deletion")

        # Update final report status and message
        sync_results['status'] = 'success'
        sync_results['message'] = (
            f"ODS restricted datasets synchronization completed with {sync_results['counts']['total']} changes: "
            f"{sync_results['counts']['created']} created, {sync_results['counts']['updated']} updated, "
            f"{sync_results['counts']['unchanged']} unchanged, {sync_results['counts']['deleted']} deleted, "
            f"{sync_results['counts']['errors']} errors. "
            f"Skipped {sync_results['counts']['skipped_unrestricted']} unrestricted and "
            f"{sync_results['counts']['skipped_not_working']} non-WORKING datasets."
        )

    except Exception as e:
        error_message = str(e)
        error_traceback = traceback.format_exc()
        logging.error(f"Exception occurred during synchronization: {error_message}")
        logging.error(f"Traceback: {error_traceback}")

        sync_results['status'] = 'error'
        sync_results['message'] = (
            f"ODS restricted datasets synchronization failed after processing {total_processed} datasets. "
            f"Error: {error_message}. "
            f"Changes before failure: {sync_results['counts']['total']} total - "
            f"{sync_results['counts']['created']} created, {sync_results['counts']['updated']} updated, "
            f"{sync_results['counts']['deleted']} deleted."
        )

    finally:
        sync_results['counts']['processed'] = total_processed

        logging.info(f"Completed processing {total_processed} restricted datasets")

        # Write detailed report to file for email/reference purposes
        try:
            current_file_path = os.path.abspath(__file__)
            project_root = os.path.dirname(os.path.dirname(current_file_path))

            reports_dir = os.path.join(project_root, "reports")
            os.makedirs(reports_dir, exist_ok=True)

            timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
            report_filename = os.path.join(reports_dir, f"ods_restricted_datasets_sync_report_{timestamp}.json")

            with open(report_filename, 'w', encoding='utf-8') as f:
                json.dump(sync_results, f, indent=2, ensure_ascii=False)
            logging.info("")
            logging.info(f"Detailed report saved to {report_filename}")
        except Exception as report_error:
            logging.error(f"Failed to write report file: {str(report_error)}")

        # Create email content
        email_subject, email_content, should_send = create_email_content(sync_results=sync_results)

        # Print a detailed report to the logs
        log_detailed_sync_report(sync_results)

        # Send email if there were datasets processed or errors
        if should_send:
            try:
                attachment = report_filename if report_filename and os.path.exists(report_filename) else None
                msg = email_helpers.create_email_msg(
                    subject=email_subject,
                    text=email_content,
                    attachment=attachment
                )
                email_helpers.send_email(msg, technical_only=True)
                logging.info("Email notification sent successfully")
            except Exception as e:
                logging.error(f"Failed to send email notification: {str(e)}")
                logging.info("Continuing execution despite email failure")
        else:
            logging.info("No restricted datasets were processed - email notification not sent")

        if sync_results['status'] == 'error':
            logging.info("ODS restricted datasets synchronization process finished with errors")
            logging.info("===============================================")
            return processed_ids

    logging.info("ODS restricted datasets synchronization process finished")
    logging.info("===============================================")

    return processed_ids


def log_detailed_sync_report(sync_results):
    """
    Log a detailed report of the restricted datasets synchronization results.

    Args:
        sync_results (dict): The synchronization results dictionary
    """
    logging.info("===== DETAILED ODS RESTRICTED DATASETS SYNC REPORT =====")
    logging.info(f"Status: {sync_results['status']}")
    logging.info(f"Message: {sync_results['message']}")
    logging.info(f"Total datasets processed: {sync_results['counts']['processed']}")
    logging.info(f"Changes: {sync_results['counts']['total']} total - "
               f"{sync_results['counts']['created']} created, "
               f"{sync_results['counts']['updated']} updated, "
               f"{sync_results['counts']['unchanged']} unchanged, "
               f"{sync_results['counts']['deleted']} deleted, "
               f"{sync_results['counts']['errors']} errors")
    logging.info(f"Skipped: {sync_results['counts']['skipped_unrestricted']} unrestricted, "
               f"{sync_results['counts']['skipped_not_working']} non-WORKING")

    if sync_results['details']['deletions']['count'] > 0:
        logging.info("")
        logging.info("--- DELETED DATASETS ---")
        for deletion in sync_results['details']['deletions']['items']:
            ods_id = deletion.get('ods_id', 'Unknown')
            title = deletion.get('title', 'Unknown')
            dataspot_link = deletion.get('link', '')
            logging.info(f"ODS dataset {ods_id}: {title} (Link: {dataspot_link})")

    if sync_results['details']['updates']['count'] > 0:
        logging.info("")
        logging.info("--- UPDATED DATASETS ---")
        for update in sync_results['details']['updates']['items']:
            ods_id = update.get('odsDataportalId', 'Unknown')
            title = update.get('title', 'Unknown')
            uuid = update.get('uuid', '')
            dataspot_link = f"{config.base_url}/web/{config.database_name}/datasets/{uuid}" if uuid else update.get('link', '')
            logging.info(f"ODS dataset {ods_id}: {title} (Link: {dataspot_link})")

            if 'changes' in update:
                for field, values in update['changes'].items():
                    logging.info(f"- {field}")
                    logging.info(f"  - Old value: {values.get('old_value', 'None')}")
                    logging.info(f"  - New value: {values.get('new_value', 'None')}")

    if sync_results['details']['creations']['count'] > 0:
        logging.info("")
        logging.info("--- CREATED DATASETS ---")
        for creation in sync_results['details']['creations']['items']:
            ods_id = creation.get('odsDataportalId', 'Unknown')
            title = creation.get('title', 'Unknown')
            uuid = creation.get('uuid', '')
            dataspot_link = f"{config.base_url}/web/{config.database_name}/datasets/{uuid}" if uuid else creation.get('link', '')
            logging.info(f"ODS dataset {ods_id}: {title} (Link: {dataspot_link})")

    if sync_results['details']['errors']['count'] > 0:
        logging.info("")
        logging.info("--- ERRORS ---")
        for error in sync_results['details']['errors']['items']:
            ods_id = error.get('ods_id', 'Unknown')
            message = error.get('message', 'Unknown error')
            logging.info(f"Error processing dataset {ods_id}: {message}")

    logging.info("=============================================")


def create_email_content(sync_results):
    """
    Create email content based on restricted datasets synchronization results.

    Args:
        sync_results (dict): Synchronization result data

    Returns:
        tuple: (email_subject, email_text, should_send)
    """
    counts = sync_results['counts']
    total_changes = counts['total']

    is_error = sync_results['status'] == 'error'

    if total_changes == 0 and counts.get('errors', 0) == 0 and not is_error:
        return None, None, False

    if is_error:
        email_subject = f"[ERROR][{config.database_name}] ODS Restricted Datasets: Processing failed after {counts['processed']} datasets"
    else:
        email_subject = f"[{config.database_name}] ODS Restricted Datasets: {counts['created']} created, {counts['updated']} updated, {counts['deleted']} deleted"
        if counts.get('errors', 0) > 0:
            email_subject += f", {counts['errors']} errors"

    email_text = f"Hi there,\n\n"

    if is_error:
        email_text += f"There was an error during the ODS restricted datasets synchronization.\n"
        email_text += f"The process failed after processing {counts['processed']} datasets.\n"
        email_text += f"Here's a summary of what was processed before the failure:\n\n"
    else:
        email_text += f"I've just synchronized restricted (unpublished) ODS datasets with Dataspot.\n"
        email_text += f"Here's a summary of the synchronization:\n\n"

    email_text += f"Changes: {counts['total']} total\n"
    email_text += f"- Created: {counts['created']} datasets\n"
    email_text += f"- Updated: {counts['updated']} datasets\n"
    email_text += f"- Unchanged: {counts['unchanged']} datasets\n"
    email_text += f"- Deleted: {counts['deleted']} datasets\n"
    if counts.get('errors', 0) > 0:
        email_text += f"- Errors: {counts['errors']}\n"
    email_text += f"\nSkipped: {counts.get('skipped_unrestricted', 0)} unrestricted, {counts.get('skipped_not_working', 0)} non-WORKING\n"
    email_text += f"\nTotal datasets processed: {counts['processed']}\n\n"

    if sync_results['details']['deletions']['count'] > 0:
        email_text += "\nDELETED DATASETS:\n"
        for deletion in sync_results['details']['deletions']['items']:
            ods_id = deletion.get('ods_id', 'Unknown')
            title = deletion.get('title', 'Unknown')
            dataspot_link = deletion.get('link', '')
            email_text += f"\nODS dataset {ods_id}: {title} (Link: {dataspot_link})\n"

    if sync_results['details']['updates']['count'] > 0:
        email_text += "\nUPDATED DATASETS:\n"
        for update in sync_results['details']['updates']['items']:
            ods_id = update.get('odsDataportalId', 'Unknown')
            title = update.get('title', 'Unknown')
            uuid = update.get('uuid', '')
            dataspot_link = f"{config.base_url}/web/{config.database_name}/datasets/{uuid}" if uuid else update.get('link', '')
            email_text += f"\nODS dataset {ods_id}: {title} (Link: {dataspot_link})\n"

            if 'changes' in update:
                for field, values in update['changes'].items():
                    email_text += f"- {field}\n"
                    email_text += f"  - Old value: {values.get('old_value', 'None')}\n"
                    email_text += f"  - New value: {values.get('new_value', 'None')}\n"

    if sync_results['details']['creations']['count'] > 0:
        email_text += "\nCREATED DATASETS:\n"
        for creation in sync_results['details']['creations']['items']:
            ods_id = creation.get('odsDataportalId', 'Unknown')
            title = creation.get('title', 'Unknown')
            uuid = creation.get('uuid', '')
            dataspot_link = f"{config.base_url}/web/{config.database_name}/datasets/{uuid}" if uuid else creation.get('link', '')
            email_text += f"\nODS dataset {ods_id}: {title} (Link: {dataspot_link})\n"

    if sync_results['details']['errors']['count'] > 0:
        email_text += "\nERRORS:\n"
        for error in sync_results['details']['errors']['items']:
            ods_id = error.get('ods_id', 'Unknown')
            message = error.get('message', 'Unknown error')
            email_text += f"\nODS dataset {ods_id}: {message}\n"

    if is_error:
        email_text += "\nThe synchronization process did not complete successfully. "
        email_text += "Please check the logs for more details.\n\n"
    else:
        email_text += "\nPlease review the synchronization results in Dataspot.\n\n"

    email_text += "Best regards,\n"
    email_text += "Your Dataspot ODS Restricted Datasets Sync Assistant"

    return email_subject, email_text, True


if __name__ == '__main__':
    if config.logging_for_prod:
        logging.basicConfig(level=logging.INFO)
    else:
        logging.basicConfig(
            level=logging.INFO,
            format="%(asctime)s [%(levelname)s] %(filename)s:%(lineno)d %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S",
        )
    logging.info(f"=== CURRENT DATABASE: {config.database_name} ===")
    logging.info(f'Executing {__file__}...')
    main()
