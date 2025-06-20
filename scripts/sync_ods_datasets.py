import logging
import json
import os
import datetime

import config
from src.clients.dnk_client import DNKClient
from src.common import email_helpers as email_helpers
import ods_utils_py as ods_utils
from src.dataset_transformer import transform_ods_to_dnk


def main():
    logging.info(f"=== CURRENT DATABASE: {config.database_name} ===")
    sync_ods_datasets()


def sync_ods_datasets(max_datasets: int = None, batch_size: int = 50):
    """
    Synchronize ODS datasets with Dataspot using DNKClient.
    
    This method:
    1. Creates a DNKClient instance for ODS dataset synchronization
    2. Retrieves public dataset IDs from ODS
    3. For each dataset, retrieves metadata and transforms it
    4. Processes datasets in batches to avoid memory issues
    5. Uses the sync_datasets method to properly update existing datasets
    6. Processes deletions by identifying datasets no longer in ODS
    7. Provides a summary of changes and logs a detailed report
    8. Sends an email notification if there were changes
    
    Args:
        max_datasets (int, optional): Maximum number of datasets to process. Defaults to None (all datasets).
        batch_size (int, optional): Number of datasets to process in each batch. Defaults to 50.
    """
    logging.info("Starting ODS datasets synchronization...")

    # Initialize Dataspot client
    dataspot_client = DNKClient()

    # Get all public dataset IDs
    logging.info(f"Step 1: Retrieving {max_datasets or 'all'} public dataset IDs from ODS...")
    ods_ids = ods_utils.get_all_dataset_ids(include_restricted=False, max_datasets=max_datasets)
    logging.info(f"Found {len(ods_ids)} datasets to process")
    
    dataspot_client.ensure_ods_imports_collection_exists()
    
    # Process datasets
    logging.info("Step 2: Processing datasets - downloading metadata and transforming...")
    total_processed = 0
    total_successful = 0
    total_failed = 0
    processed_ids = []
    all_datasets = []
    
    # Store sync results for reporting
    sync_results = {
        'status': 'pending',
        'message': '',
        'counts': {
            'total': 0,
            'created': 0,
            'updated': 0,
            'deleted': 0,
            'unchanged': 0,
            'errors': 0,
            'processed': 0
        },
        'details': {
            'creations': {
                'count': 0,
                'items': []
            },
            'updates': {
                'count': 0,
                'items': []
            },
            'deletions': {
                'count': 0,
                'items': []
            },
            'errors': {
                'count': 0,
                'items': []
            }
        }
    }
    
    # Keep a master set of all ODS IDs we've processed
    all_processed_ods_ids = set()
    
    for idx, ods_id in enumerate(ods_ids):
        logging.info(f"[{idx+1}/{len(ods_ids)}] Processing dataset {ods_id}...")
        
        # Get metadata from ODS and transform to Dataspot dataset
        ods_metadata = ods_utils.get_dataset_metadata(dataset_id=ods_id)
        dataset = transform_ods_to_dnk(ods_metadata=ods_metadata, ods_dataset_id=ods_id)
        
        # Add to collection
        all_datasets.append(dataset)
        processed_ids.append(ods_id)
        all_processed_ods_ids.add(ods_id)
        
        logging.info(f"Successfully transformed dataset {ods_id}: {dataset.name}")
        total_successful += 1
        total_processed += 1
        
        # Process in smaller batches to avoid memory issues
        if len(all_datasets) >= batch_size or idx == len(ods_ids) - 1:
            if all_datasets:
                batch_num = len(all_datasets)
                logging.info(f"Step 3: Syncing batch of {batch_num} datasets...")
                
                # Sync datasets - the method handles updates properly
                sync_summary = dataspot_client.sync_datasets(datasets=all_datasets)
                
                logging.info(f"Batch sync completed. Response summary: {sync_summary}")
                
                # Update overall counts
                sync_results['counts']['created'] += sync_summary.get('created', 0)
                sync_results['counts']['updated'] += sync_summary.get('updated', 0)
                sync_results['counts']['deleted'] += sync_summary.get('deleted', 0)
                sync_results['counts']['errors'] += sync_summary.get('errors', 0)
                sync_results['counts']['unchanged'] += sync_summary.get('unchanged', 0)
                sync_results['counts']['total'] += (
                    sync_summary.get('created', 0) + 
                    sync_summary.get('updated', 0) + 
                    sync_summary.get('deleted', 0)
                )
                
                # Append detailed change information to the report
                if 'details' in sync_summary:
                    # Merge creations
                    if 'creations' in sync_summary['details']:
                        sync_results['details']['creations']['count'] += sync_summary['details']['creations'].get('count', 0)
                        sync_results['details']['creations']['items'].extend(sync_summary['details']['creations'].get('items', []))
                    
                    # Merge updates
                    if 'updates' in sync_summary['details']:
                        sync_results['details']['updates']['count'] += sync_summary['details']['updates'].get('count', 0)
                        sync_results['details']['updates']['items'].extend(sync_summary['details']['updates'].get('items', []))
                    
                    # Merge deletions
                    if 'deletions' in sync_summary['details']:
                        sync_results['details']['deletions']['count'] += sync_summary['details']['deletions'].get('count', 0)
                        sync_results['details']['deletions']['items'].extend(sync_summary['details']['deletions'].get('items', []))
                    
                    # Merge errors
                    if 'errors' in sync_summary['details']:
                        sync_results['details']['errors']['count'] += sync_summary['details']['errors'].get('count', 0)
                        sync_results['details']['errors']['items'].extend(sync_summary['details']['errors'].get('items', []))
                
                # Clear the batch for the next iteration
                all_datasets = []

    # After all batches have been processed, handle deletions
    logging.info("Step 4: Processing deletions - identifying datasets no longer in ODS...")
    
    # Get all existing ODS dataset IDs from Dataspot using the asset filter
    logging.info("Getting all ODS dataset IDs from Dataspot...")

    # Define a filter function to get only datasets with ODS_ID
    ods_filter = lambda asset: (
        asset.get('_type') == 'Dataset' and
        asset.get('ODS_ID') is not None and
        asset.get('status') not in ['INTERMINATION2']) # Ignore archived assets
    
    # Get all datasets from Dataspot with ODS_ID
    all_dataspot_datasets = dataspot_client.get_all_assets_from_scheme(filter_function=ods_filter)
    
    # Extract ODS IDs from the datasets
    dataspot_ods_ids = set()
    for dataset in all_dataspot_datasets:
        ods_id = dataset.get('ODS_ID')
        if ods_id:
            dataspot_ods_ids.add(ods_id)
    
    logging.info(f"Found {len(dataspot_ods_ids)} datasets with ODS_ID in Dataspot")
    
    # Find datasets that are in Dataspot but not in the current ODS fetch
    datasets_to_delete = dataspot_ods_ids - all_processed_ods_ids
    
    if datasets_to_delete:
        logging.info(f"Found {len(datasets_to_delete)} datasets to mark for deletion")
        
        # Process each dataset for deletion
        for ods_id in datasets_to_delete:
            try:
                # Call delete_dataset to mark it for deletion
                deleted = dataspot_client.dataset_handler.delete_dataset(ods_id, fail_if_not_exists=False)
                
                if deleted:
                    # Track deletion in results
                    sync_results['counts']['deleted'] += 1
                    sync_results['counts']['total'] += 1
                    sync_results['details']['deletions']['count'] += 1
                    
                    # Find the dataset info from all_dataspot_datasets
                    dataset_info = next((d for d in all_dataspot_datasets if d.get('ODS_ID') == ods_id), None)
                    
                    if dataset_info:
                        title = dataset_info.get('label', f"<Unnamed Dataset {ods_id}>")
                        uuid = dataset_info.get('id')
                        
                        # Create Dataspot link
                        dataspot_link = f"{config.base_url}/web/{config.database_name}/datasets/{uuid}" if uuid else ''
                        
                        # Add to deletion details
                        deletion_entry = {
                            "ods_id": ods_id,
                            "title": title,
                            "uuid": uuid,
                            "link": dataspot_link
                        }
                        
                        sync_results['details']['deletions']['items'].append(deletion_entry)
                        logging.info(f"Marked dataset with ODS_ID {ods_id} for deletion: {title} (Link: {dataspot_link})")
                    else:
                        # Fallback if dataset info not found
                        sync_results['details']['deletions']['items'].append({
                            "ods_id": ods_id,
                            "title": f"<Unnamed Dataset {ods_id}>",
                            "uuid": "",
                            "link": ""
                        })
                        logging.info(f"Marked dataset with ODS_ID {ods_id} for deletion")
                
            except Exception as e:
                error_msg = f"Error marking dataset with ODS_ID {ods_id} for deletion: {str(e)}"
                logging.error(error_msg)
                
                sync_results['counts']['errors'] += 1
                sync_results['details']['errors']['count'] += 1
                sync_results['details']['errors']['items'].append({
                    "ods_id": ods_id,
                    "message": error_msg
                })
    else:
        logging.info("No datasets found for deletion")

    # Update final report status and message
    sync_results['status'] = 'success'
    sync_results['message'] = (
        f"ODS datasets synchronization completed with {sync_results['counts']['total']} changes: "
        f"{sync_results['counts']['created']} created, {sync_results['counts']['updated']} updated, "
        f"{sync_results['counts']['unchanged']} unchanged, {sync_results['counts']['deleted']} deleted."
    )
    
    # Update final counts (processed may differ from total changes)
    sync_results['counts']['processed'] = total_processed
    
    # Log final summary
    logging.info(f"Completed processing {total_processed} datasets: {total_successful} successful, {total_failed} failed")
    
    # Write detailed report to file for email/reference purposes
    # Get project root directory (one level up from scripts)
    current_file_path = os.path.abspath(__file__)
    project_root = os.path.dirname(os.path.dirname(current_file_path))
    
    # Define reports directory in project root
    reports_dir = os.path.join(project_root, "reports")
    
    # Create reports directory if it doesn't exist
    os.makedirs(reports_dir, exist_ok=True)
    
    # Generate filename with timestamp
    timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
    report_filename = os.path.join(reports_dir, f"ods_datasets_sync_report_{timestamp}.json")
    
    # Write report to file
    with open(report_filename, 'w', encoding='utf-8') as f:
        json.dump(sync_results, f, indent=2, ensure_ascii=False)

    logging.info("")
    logging.info(f"Detailed report saved to {report_filename}")

    # Create email content
    email_subject, email_content, should_send = create_email_content(
        sync_results=sync_results,
        scheme_name_short=dataspot_client.database_name
    )
    
    # Print a detailed report to the logs
    log_detailed_sync_report(sync_results)
    
    # Send email if there were datasets processed
    if should_send:
        try:
            # Create and send email
            attachment = report_filename if os.path.exists(report_filename) else None
            msg = email_helpers.create_email_msg(
                subject=email_subject,
                text=email_content,
                attachment=attachment
            )
            email_helpers.send_email(msg)
            logging.info("Email notification sent successfully")
        except Exception as e:
            # Log error but continue execution
            logging.error(f"Failed to send email notification: {str(e)}")
            logging.info("Continuing execution despite email failure")
    else:
        logging.info("No datasets were processed - email notification not sent")
    
    logging.info("ODS datasets synchronization process finished")
    logging.info("===============================================")
    
    return processed_ids


def log_detailed_sync_report(sync_results):
    """
    Log a detailed report of the synchronization results.
    
    Args:
        sync_results (dict): The synchronization results dictionary
    """
    logging.info("===== DETAILED ODS DATASETS SYNC REPORT =====")
    logging.info(f"Status: {sync_results['status']}")
    logging.info(f"Message: {sync_results['message']}")
    logging.info(f"Total datasets processed: {sync_results['counts']['processed']}")
    logging.info(f"Changes: {sync_results['counts']['total']} total - "
               f"{sync_results['counts']['created']} created, "
               f"{sync_results['counts']['updated']} updated, "
               f"{sync_results['counts']['unchanged']} unchanged, "
               f"{sync_results['counts']['deleted']} deleted, "
               f"{sync_results['counts']['errors']} errors")
    
    # Log detailed information about deleted datasets
    if sync_results['details']['deletions']['count'] > 0:
        logging.info("")
        logging.info("--- DELETED DATASETS ---")
        for deletion in sync_results['details']['deletions']['items']:
            ods_id = deletion.get('ods_id', 'Unknown')
            title = deletion.get('title', 'Unknown')
            dataspot_link = deletion.get('link', '')
            
            # Show link directly after title in brackets
            logging.info(f"ODS dataset {ods_id}: {title} (Link: {dataspot_link})")
    
    # Log detailed information about updated datasets
    if sync_results['details']['updates']['count'] > 0:
        logging.info("")
        logging.info("--- UPDATED DATASETS ---")
        for update in sync_results['details']['updates']['items']:
            ods_id = update.get('ods_id', 'Unknown')
            title = update.get('title', 'Unknown')
            uuid = update.get('uuid', '')
            
            # Create Dataspot link instead of ODS source link
            dataspot_link = f"{config.base_url}/web/{config.database_name}/datasets/{uuid}" if uuid else update.get('link', '')

            # Show link directly after title in brackets
            logging.info(f"ODS dataset {ods_id}: {title} (Link: {dataspot_link})")
            
            # Log field changes
            if 'changes' in update:
                for field, values in update['changes'].items():
                    logging.info(f"- {field}")
                    logging.info(f"  - Old value: {values.get('old_value', 'None')}")
                    logging.info(f"  - New value: {values.get('new_value', 'None')}")
    
    # Log detailed information about created datasets
    if sync_results['details']['creations']['count'] > 0:
        logging.info("")
        logging.info("--- CREATED DATASETS ---")
        for creation in sync_results['details']['creations']['items']:
            ods_id = creation.get('ods_id', 'Unknown')
            title = creation.get('title', 'Unknown')
            uuid = creation.get('uuid', '')
            
            # Create Dataspot link instead of ODS source link
            dataspot_link = f"{config.base_url}/web/{config.database_name}/datasets/{uuid}" if uuid else creation.get('link', '')
            
            # Show link directly after title in brackets
            logging.info(f"ODS dataset {ods_id}: {title} (Link: {dataspot_link})")
    
    # Log detailed information about errors
    if sync_results['details']['errors']['count'] > 0:
        logging.info("")
        logging.info("--- ERRORS ---")
        for error in sync_results['details']['errors']['items']:
            ods_id = error.get('ods_id', 'Unknown')
            message = error.get('message', 'Unknown error')
            
            logging.info(f"Error processing dataset {ods_id}: {message}")
    
    logging.info("=============================================")


def create_email_content(sync_results, scheme_name_short):
    """
    Create email content based on synchronization results.

    Args:
        sync_results (dict): Synchronization result data
        scheme_name_short (str): Short name of the scheme (database name)

    Returns:
        tuple: (email_subject, email_text, should_send)
    """
    counts = sync_results['counts']
    total_changes = counts['total']
    
    # Only create email if there were changes
    if total_changes == 0 and counts.get('errors', 0) == 0:
        return None, None, False
    
    # Create email subject with summary following the requested format
    email_subject = f"[{scheme_name_short}] ODS Datasets: {counts['created']} created, {counts['updated']} updated, {counts['deleted']} deleted"
    if counts.get('errors', 0) > 0:
        email_subject += f", {counts['errors']} errors"
    
    email_text = f"Hi there,\n\n"
    email_text += f"I've just synchronized ODS datasets with Dataspot.\n"
    email_text += f"Here's a summary of the synchronization:\n\n"
    
    # Add summary counts
    email_text += f"Changes: {counts['total']} total\n"
    email_text += f"- Created: {counts['created']} datasets\n"
    email_text += f"- Updated: {counts['updated']} datasets\n"
    email_text += f"- Unchanged: {counts['unchanged']} datasets\n"
    email_text += f"- Deleted: {counts['deleted']} datasets\n"
    if counts.get('errors', 0) > 0:
        email_text += f"- Errors: {counts['errors']}\n"
    email_text += f"\nTotal datasets processed: {counts['processed']}\n\n"
    
    # Add detailed information if available
    
    # Add information about deleted datasets FIRST
    if sync_results['details']['deletions']['count'] > 0:
        email_text += "\nDELETED DATASETS:\n"
        for deletion in sync_results['details']['deletions']['items']:
            ods_id = deletion.get('ods_id', 'Unknown')
            title = deletion.get('title', 'Unknown')
            dataspot_link = deletion.get('link', '')
            
            # Show link directly after title in brackets
            email_text += f"\nODS dataset {ods_id}: {title} (Link: {dataspot_link})\n"
    
    # Then show updated datasets
    if sync_results['details']['updates']['count'] > 0:
        email_text += "\nUPDATED DATASETS:\n"
        for update in sync_results['details']['updates']['items']:
            ods_id = update.get('ods_id', 'Unknown')
            title = update.get('title', 'Unknown')
            uuid = update.get('uuid', '')
            
            # Create Dataspot link instead of ODS source link
            dataspot_link = f"{config.base_url}/web/{config.database_name}/datasets/{uuid}" if uuid else update.get('link', '')
            
            # Show link directly after title in brackets
            email_text += f"\nODS dataset {ods_id}: {title} (Link: {dataspot_link})\n"
            
            # Add field changes
            if 'changes' in update:
                for field, values in update['changes'].items():
                    email_text += f"- {field}\n"
                    email_text += f"  - Old value: {values.get('old_value', 'None')}\n"
                    email_text += f"  - New value: {values.get('new_value', 'None')}\n"
    
    # Finally show created datasets LAST
    if sync_results['details']['creations']['count'] > 0:
        email_text += "\nCREATED DATASETS:\n"
        for creation in sync_results['details']['creations']['items']:
            ods_id = creation.get('ods_id', 'Unknown')
            title = creation.get('title', 'Unknown')
            uuid = creation.get('uuid', '')
            
            # Create Dataspot link instead of ODS source link
            dataspot_link = f"{config.base_url}/web/{config.database_name}/datasets/{uuid}" if uuid else creation.get('link', '')
            
            # Show link directly after title in brackets
            email_text += f"\nODS dataset {ods_id}: {title} (Link: {dataspot_link})\n"
    
    email_text += "\nPlease review the synchronization results in Dataspot.\n\n"
    email_text += "Best regards,\n"
    email_text += "Your Dataspot ODS Datasets Sync Assistant"
    
    return email_subject, email_text, True


if __name__ == '__main__':
    logging.basicConfig(
        level=logging.INFO,
        format='%(levelname)s:%(name)s:[%(filename)s:%(funcName)s:%(lineno)d] %(message)s'
    )
    logging.info(f'Executing {__file__}...')
    main()
