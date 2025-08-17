import logging
import json
import os
import datetime
import time
import traceback

import config
from src.clients.tdm_client import TDMClient
from src.ods_client import ODSClient
from src.common import email_helpers as email_helpers
import ods_utils_py as ods_utils


def main():
    sync_ods_dataset_components()


def sync_ods_dataset_components(max_datasets: int = None, batch_size: int = 50):
    """
    Synchronize ODS dataset components (columns) with Dataspot using TDMClient.
    
    This method:
    1. Creates a TDMClient instance for ODS dataset component synchronization
    2. Retrieves public dataset IDs from ODS
    3. For each dataset, retrieves column information and creates/updates TDM dataobjects
    4. Processes datasets in batches to avoid memory issues
    5. Provides a summary of changes and logs a detailed report
    6. Sends an email notification if there were changes
    
    Args:
        max_datasets (int, optional): Maximum number of datasets to process. Defaults to None (all datasets).
        batch_size (int, optional): Number of datasets to process in each batch. Defaults to 50.
    """
    logging.info("Starting ODS dataset components synchronization...")

    # Initialize clients
    tdm_client = TDMClient()
    ods_client = ODSClient()
    
    # Store sync results for reporting
    sync_results = {
        'status': 'pending',
        'message': '',
        'counts': {
            'total_processed': 0,
            'total_changes': 0,
            'created': 0,
            'updated': 0,
            'unchanged': 0,
            'errors': 0,
            'attributes_created': 0,
            'attributes_updated': 0,
            'attributes_deleted': 0,
            'attributes_unchanged': 0
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
            'errors': {
                'count': 0,
                'items': []
            }
        }
    }
    
    # Track processing
    total_processed = 0
    total_successful = 0
    total_failed = 0
    report_filename = None
    
    try:
        # Get all public dataset IDs
        logging.info(f"Step 1: Retrieving {max_datasets or 'all'} public dataset IDs from ODS...")
        ods_ids = ods_utils.get_all_dataset_ids(include_restricted=False, max_datasets=max_datasets)
        logging.info(f"Found {len(ods_ids)} datasets to process")
        
        # Process datasets
        logging.info("Step 2: Processing dataset components - downloading column information and creating TDM objects...")
        
        # Process datasets in batches
        for batch_start in range(0, len(ods_ids), batch_size):
            batch_end = min(batch_start + batch_size, len(ods_ids))
            current_batch = ods_ids[batch_start:batch_end]
            batch_num = batch_start // batch_size + 1
            total_batches = (len(ods_ids) + batch_size - 1) // batch_size
            
            logging.info(f"Processing batch {batch_num}/{total_batches} with {len(current_batch)} datasets...")
            
            for idx, ods_id in enumerate(current_batch):
                logging.info(f"[{batch_start + idx + 1}/{len(ods_ids)}] Processing dataset {ods_id}...")
                logging.info(f"First, waiting 10 seconds for server to cool down")
                time.sleep(10)
                
                try:
                    # Get dataset title
                    dataset_title = ods_utils.get_dataset_title(dataset_id=ods_id)
                    if not dataset_title:
                        error_msg = f"Could not retrieve title for dataset {ods_id}"
                        logging.error(error_msg)
                        
                        # Track error
                        sync_results['counts']['errors'] += 1
                        sync_results['details']['errors']['count'] += 1
                        sync_results['details']['errors']['items'].append({
                            'ods_id': ods_id,
                            'message': error_msg
                        })
                        total_failed += 1
                        continue
                    
                    logging.info(f"Retrieved dataset title: '{dataset_title}'")
                    
                    # Get dataset columns
                    columns = ods_client.get_dataset_columns(dataset_id=ods_id)
                    if not columns:
                        logging.warning(f"No columns found for dataset {ods_id}: {dataset_title}")
                        columns = []  # Use empty list to create dataobject without attributes
                    else:
                        logging.info(f"Retrieved {len(columns)} columns for dataset {ods_id}")
                    
                    # Sync dataset components
                    logging.info(f"Synchronizing dataset components for {ods_id}: '{dataset_title}'")
                    result = tdm_client.sync_dataset_components(ods_id=ods_id, name=dataset_title, columns=columns)
                    
                    # Parse result
                    is_new = result.get('is_new', False)
                    
                    # Update counts based on result
                    if is_new:
                        sync_results['counts']['created'] += 1
                        sync_results['counts']['total_changes'] += 1
                        sync_results['details']['creations']['count'] += 1
                        logging.info(f"Created new dataobject for dataset {ods_id}: '{dataset_title}' with {len(columns)} columns")
                        
                        # Store creation details
                        creation_item = {
                            'ods_id': ods_id,
                            'title': dataset_title,
                            'uuid': result.get('uuid'),
                            'link': result.get('link', ''),
                            'columns_count': len(columns),
                            'attributes_created': result.get('counts', {}).get('created_attributes', 0)
                        }
                        
                        # Add created attributes details if available
                        if 'details' in result and 'created_attributes' in result['details']:
                            creation_item['created_fields'] = {}
                            for attr in result['details']['created_attributes']:
                                attr_name = attr.get('name', 'unknown')
                                creation_item['created_fields'][attr_name] = {
                                    'description': {
                                        'new_value': attr.get('description', '')
                                    },
                                    'type': {
                                        'new_value': attr.get('type', '')
                                    }
                                }
                        
                        sync_results['details']['creations']['items'].append(creation_item)
                    else:
                        # Check if any attributes were actually modified
                        attrs_created = result.get('counts', {}).get('created_attributes', 0)
                        attrs_updated = result.get('counts', {}).get('updated_attributes', 0)
                        attrs_deleted = result.get('counts', {}).get('deleted_attributes', 0)
                        attrs_modified = attrs_created + attrs_updated + attrs_deleted
                        
                        if attrs_modified == 0:
                            sync_results['counts']['unchanged'] += 1
                            logging.info(f"Dataobject for dataset {ods_id}: '{dataset_title}' is unchanged (all {len(columns)} columns match)")
                        else:
                            sync_results['counts']['updated'] += 1
                            sync_results['counts']['total_changes'] += 1
                            sync_results['details']['updates']['count'] += 1
                            
                            # Log the changes
                            changes = []
                            if attrs_created > 0:
                                changes.append(f"{attrs_created} columns created")
                            if attrs_updated > 0:
                                changes.append(f"{attrs_updated} columns updated")
                            if attrs_deleted > 0:
                                changes.append(f"{attrs_deleted} columns deleted")
                                
                            logging.info(f"Updated dataobject for dataset {ods_id}: '{dataset_title}' with changes: {', '.join(changes)}")
                        
                            # Store update details in a more structured format similar to other handlers
                            update_item = {
                                'ods_id': ods_id,
                                'title': dataset_title,
                                'uuid': result.get('uuid'),
                                'link': result.get('link', ''),
                                'columns_count': len(columns),
                                'created_attrs': attrs_created,
                                'updated_attrs': attrs_updated,
                                'deleted_attrs': attrs_deleted,
                                'unchanged_attrs': result.get('counts', {}).get('unchanged_attributes', 0)
                            }
                            
                            # Include detailed field changes if available
                            field_changes = result.get('details', {}).get('field_changes', {})
                            if field_changes:
                                update_item['changed_fields'] = field_changes
                            
                            # Include newly created attributes if available
                            if attrs_created > 0 and 'details' in result and 'created_attributes' in result['details']:
                                if 'changed_fields' not in update_item:
                                    update_item['changed_fields'] = {}
                                
                                for attr in result['details']['created_attributes']:
                                    attr_name = attr.get('name', 'unknown')
                                    update_item['changed_fields'][attr_name] = {
                                        'description': {
                                            'new_value': attr.get('description', '')
                                        },
                                        'type': {
                                            'new_value': attr.get('type', '')
                                        }
                                    }
                                
                            sync_results['details']['updates']['items'].append(update_item)
                    
                    # Update attribute counts
                    sync_results['counts']['attributes_created'] += result.get('counts', {}).get('created_attributes', 0)
                    sync_results['counts']['attributes_updated'] += result.get('counts', {}).get('updated_attributes', 0)
                    sync_results['counts']['attributes_deleted'] += result.get('counts', {}).get('deleted_attributes', 0)
                    sync_results['counts']['attributes_unchanged'] += result.get('counts', {}).get('unchanged_attributes', 0)
                    
                    # Log success
                    logging.info(f"Successfully processed components for dataset {ods_id}: {dataset_title}")
                    total_successful += 1
                    
                except Exception as e:
                    error_msg = f"Error processing components for dataset {ods_id}: {str(e)}"
                    logging.error(error_msg)
                    
                    # Track error
                    sync_results['counts']['errors'] += 1
                    sync_results['details']['errors']['count'] += 1
                    sync_results['details']['errors']['items'].append({
                        'ods_id': ods_id,
                        'message': error_msg
                    })
                    total_failed += 1
                
                total_processed += 1
        
        # Update final report status and message
        sync_results['status'] = 'success'
        sync_results['message'] = (
            f"ODS dataset components synchronization completed with {sync_results['counts']['total_changes']} changes: "
            f"{sync_results['counts']['created']} new dataobjects created, "
            f"{sync_results['counts']['updated']} existing dataobjects updated, "
            f"and {sync_results['counts']['unchanged']} were unchanged. "
            f"Attribute changes: {sync_results['counts']['attributes_created']} created, "
            f"{sync_results['counts']['attributes_updated']} updated, "
            f"{sync_results['counts']['attributes_deleted']} deleted."
        )
        
    except Exception as e:
        # Capture error information
        error_message = str(e)
        error_traceback = traceback.format_exc()
        logging.error(f"Exception occurred during synchronization: {error_message}")
        logging.error(f"Traceback: {error_traceback}")
        
        # Update the sync_results with error status
        sync_results['status'] = 'error'
        sync_results['message'] = (
            f"ODS dataset components synchronization failed after processing {total_processed} datasets. "
            f"Error: {error_message}. "
            f"Successfully processed: {total_successful}, errors: {total_failed}. "
            f"Changes before failure: {sync_results['counts']['total_changes']} total - "
            f"{sync_results['counts']['created']} created, {sync_results['counts']['updated']} updated."
        )

    finally:
        # Update final counts (should happen whether successful or not)
        sync_results['counts']['total_processed'] = total_processed
        
        # Log final summary
        logging.info(f"Completed processing components for {total_processed} datasets: {total_successful} successful, {total_failed} failed")
        
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
        report_filename = os.path.join(reports_dir, f"ods_dataset_components_sync_report_{timestamp}.json")
        
        try:
            # Write report to file
            with open(report_filename, 'w', encoding='utf-8') as f:
                json.dump(sync_results, f, indent=2, ensure_ascii=False)
            logging.info("")
            logging.info(f"Detailed report saved to {report_filename}")
        except Exception as report_error:
            logging.error(f"Failed to write report file: {str(report_error)}")

        # Create email content
        email_subject, email_content, should_send = create_email_content(
            sync_results=sync_results,
            database_name=tdm_client.database_name
        )
        
        # Print a detailed report to the logs
        log_detailed_sync_report(sync_results)
        
        # Send email if there were datasets processed or errors
        if should_send:
            try:
                # Create and send email
                attachment = report_filename if os.path.exists(report_filename) else None
                msg = email_helpers.create_email_msg(
                    subject=email_subject,
                    text=email_content,
                    attachment=attachment
                )
                email_helpers.send_email(msg, technical_only=True)
                logging.info("Email notification sent successfully")
            except Exception as e:
                # Log error but continue execution
                logging.error(f"Failed to send email notification: {str(e)}")
                logging.info("Continuing execution despite email failure")
        else:
            logging.info("No datasets were processed - email notification not sent")
        
        # Re-raise the exception if we had one
        if sync_results['status'] == 'error':
            logging.info("ODS dataset components synchronization process finished with errors")
            logging.info("===============================================")
            return total_processed

    logging.info("ODS dataset components synchronization process finished")
    logging.info("===============================================")
    
    return total_processed


def log_detailed_sync_report(sync_results):
    """
    Log a detailed report of the synchronization results.
    
    Args:
        sync_results (dict): The synchronization results dictionary
    """
    logging.info("===== DETAILED ODS DATASET COMPONENTS SYNC REPORT =====")
    logging.info(f"Status: {sync_results['status']}")
    logging.info(f"Message: {sync_results['message']}")
    logging.info(f"Total datasets processed: {sync_results['counts']['total_processed']}")
    logging.info(f"Changes: "
               f"{sync_results['counts']['created']} created, "
               f"{sync_results['counts']['updated']} updated, "
               f"{sync_results['counts']['unchanged']} unchanged, "
               f"{sync_results['counts']['errors']} errors")
    logging.info(f"Attribute changes: "
               f"{sync_results['counts']['attributes_created']} created, "
               f"{sync_results['counts']['attributes_updated']} updated, "
               f"{sync_results['counts']['attributes_deleted']} deleted, "
               f"{sync_results['counts']['attributes_unchanged']} unchanged")
    
    # Log information about updated dataobjects
    if sync_results['details']['updates']['count'] > 0:
        logging.info("")
        logging.info("--- UPDATED DATAOBJECTS ---")
        for update in sync_results['details']['updates']['items']:
            if update['created_attrs'] == 0 and update['updated_attrs'] == 0 and update['deleted_attrs'] == 0:
                continue  # Skip unchanged objects
                
            ods_id = update.get('ods_id', 'Unknown')
            title = update.get('title', 'Unknown')
            uuid = update.get('uuid', 'Unknown')
            link = update.get('link', '')
            
            # Display title and link
            logging.info(f"Updated dataobject for ODS dataset {ods_id}: {title} (Link: {link})")
            
            logging.info(f"  - Created attributes: {update['created_attrs']}")
            logging.info(f"  - Updated attributes: {update['updated_attrs']}")
            logging.info(f"  - Deleted attributes: {update['deleted_attrs']}")
            logging.info(f"  - Unchanged attributes: {update['unchanged_attrs']}")
            
            # Display detailed field changes if available
            if 'changed_fields' in update and update['changed_fields']:
                logging.info("  - Changed fields:")
                for attr_name, changes in update['changed_fields'].items():
                    logging.info(f"    - Attribute: {attr_name}")
                    for field, values in changes.items():
                        old_val = values.get('old_value', 'None')
                        new_val = values.get('new_value', 'None')
                        logging.info(f"      - {field}:")
                        logging.info(f"        - Old value: '{old_val}'")
                        logging.info(f"        - New value: '{new_val}'")
    
    # Log information about created dataobjects
    if sync_results['details']['creations']['count'] > 0:
        logging.info("")
        logging.info("--- CREATED DATAOBJECTS ---")
        for creation in sync_results['details']['creations']['items']:
            ods_id = creation.get('ods_id', 'Unknown')
            title = creation.get('title', 'Unknown')
            columns_count = creation.get('columns_count', 0)
            uuid = creation.get('uuid', 'Unknown')
            link = creation.get('link', '')
            
            # Display link right after title
            logging.info(f"Created dataobject for ODS dataset {ods_id}: {title} with {columns_count} columns (Link: {link})")
            
            # Display detailed created field information if available
            if 'created_fields' in creation and creation['created_fields']:
                logging.info("  - Created fields:")
                for attr_name, details in creation['created_fields'].items():
                    logging.info(f"    - Attribute: {attr_name}")
                    for field, values in details.items():
                        new_val = values.get('new_value', 'None')
                        logging.info(f"      - {field}:")
                        logging.info(f"        - Value: '{new_val}'")
    
    # Log information about errors
    if sync_results['details']['errors']['count'] > 0:
        logging.info("")
        logging.info("--- ERRORS ---")
        for error in sync_results['details']['errors']['items']:
            ods_id = error.get('ods_id', 'Unknown')
            message = error.get('message', 'Unknown error')
            
            logging.info(f"Error processing dataset {ods_id}: {message}")
    
    logging.info("=============================================")


def create_email_content(sync_results, database_name):
    """
    Create email content based on synchronization results.

    Args:
        sync_results (dict): Synchronization result data
        database_name (str): Name of the database

    Returns:
        tuple: (email_subject, email_text, should_send)
    """
    counts = sync_results['counts']
    total_changes = counts['total_changes']
    
    # Modified to send email on error or if changes happened
    is_error = sync_results['status'] == 'error'
    
    # Send email if there were changes or errors
    if total_changes == 0 and counts.get('errors', 0) == 0 and not is_error:
        return None, None, False
    
    # Create email subject with summary
    if is_error:
        email_subject = f"[ERROR][{database_name}] ODS Dataset Components: Processing failed after {counts['total_processed']} datasets"
    else:
        email_subject = f"[{database_name}] ODS Dataset Components: {counts['created']} created, {counts['updated']} updated"
        if counts.get('errors', 0) > 0:
            email_subject += f", {counts['errors']} errors"
    
    email_text = f"Hi there,\n\n"
    
    if is_error:
        email_text += f"There was an error during the ODS dataset components synchronization.\n"
        email_text += f"The process failed after processing {counts['total_processed']} datasets.\n"
        email_text += f"Here's a summary of what was processed before the failure:\n\n"
    else:
        email_text += f"I've just synchronized ODS dataset components with Dataspot's TDM scheme.\n"
        email_text += f"Here's a summary of the synchronization:\n\n"
    
    # Add summary counts
    email_text += f"Datasets processed: {counts['total_processed']} total\n"
    email_text += f"- Created: {counts['created']} dataobjects\n"
    email_text += f"- Updated: {counts['updated']} dataobjects\n"
    email_text += f"- Unchanged: {counts['unchanged']} dataobjects\n"
    if counts.get('errors', 0) > 0:
        email_text += f"- Errors: {counts['errors']}\n"
    
    # Add attribute changes
    email_text += f"\nAttribute changes:\n"
    email_text += f"- Created: {counts['attributes_created']} attributes\n"
    email_text += f"- Updated: {counts['attributes_updated']} attributes\n"
    email_text += f"- Deleted: {counts['attributes_deleted']} attributes\n"
    email_text += f"- Unchanged: {counts['attributes_unchanged']} attributes\n"
    
    # Add information about updated dataobjects with significant changes
    significant_updates = []
    for update in sync_results['details']['updates']['items']:
        if update['created_attrs'] > 0 or update['updated_attrs'] > 0 or update['deleted_attrs'] > 0:
            significant_updates.append(update)
            
    if significant_updates:
        email_text += "\nUPDATED DATAOBJECTS WITH SIGNIFICANT CHANGES:\n"
        for update in significant_updates:
            ods_id = update.get('ods_id', 'Unknown')
            title = update.get('title', 'Unknown')
            link = update.get('link', '')
            
            # Display link right after title
            email_text += f"\n- {title} (ODS ID: {ods_id}, Link: {link})\n"
            email_text += f"  Created: {update['created_attrs']}, Updated: {update['updated_attrs']}, Deleted: {update['deleted_attrs']}\n"
            
            # Include some field change details if available
            if 'changed_fields' in update and update['changed_fields']:
                deleted_attrs = []
                updated_attrs = []
                created_attrs = []
                
                # Categorize attributes by change type
                for attr_name, changes in update['changed_fields'].items():
                    # Check if this is a deleted attribute (has old_value but no new_value for description)
                    if 'description' in changes and 'old_value' in changes['description'] and 'new_value' not in changes['description']:
                        deleted_attrs.append(attr_name)
                    # Check if this is a created attribute (has new_value but no old_value for description)
                    elif 'description' in changes and 'new_value' in changes['description'] and 'old_value' not in changes['description']:
                        created_attrs.append(attr_name)
                    # Otherwise it's an updated attribute
                    else:
                        updated_attrs.append(attr_name)
                
                # List deleted attributes
                if deleted_attrs:
                    email_text += f"  Deleted attributes: {', '.join(deleted_attrs)}\n"
                
                # List created attributes
                if created_attrs:
                    email_text += f"  Created attributes: {', '.join(created_attrs)}\n"
                
                # List updated attributes
                if updated_attrs:
                    email_text += f"  Updated attributes: {', '.join(updated_attrs)}\n"
                
                # Show detailed changes for all attributes
                for attr_name, changes in update['changed_fields'].items():
                    email_text += f"  Attribute '{attr_name}' changes:\n"
                    for field, values in changes.items():
                        old_val = values.get('old_value', 'None')
                        new_val = values.get('new_value', 'None')
                        email_text += f"    - {field}:\n"
                        email_text += f"      - Old value: '{old_val}'\n"
                        email_text += f"      - New value: '{new_val}'\n"

    
    # Include information about created dataobjects
    if sync_results['details']['creations']['count'] > 0:
        creations = sync_results['details']['creations']['items']
        email_text += "\nNEWLY CREATED DATAOBJECTS:\n"
        for creation in creations:
            ods_id = creation.get('ods_id', 'Unknown')
            title = creation.get('title', 'Unknown')
            columns_count = creation.get('columns_count', 0)
            link = creation.get('link', '')
            
            # Display link right after title
            email_text += f"\n- {title} (ODS ID: {ods_id}, Link: {link})\n"
            email_text += f"  Created with {columns_count} columns\n"
            
            # Include details about created fields if available
            if 'created_fields' in creation and creation['created_fields']:
                # First, list all attribute names in a summary
                attr_names = list(creation['created_fields'].keys())
                email_text += f"  Created attributes: {', '.join(attr_names)}\n"
                
                # Show detailed info for all attributes
                for attr_name, details in creation['created_fields'].items():
                    email_text += f"  Attribute '{attr_name}' details:\n"
                    for field, values in details.items():
                        new_val = values.get('new_value', 'None')
                        email_text += f"    - {field}: '{new_val}'\n"

    
    # Include some error information if any
    if sync_results['details']['errors']['count'] > 0:
        email_text += "\nERRORS:\n"
        for error in sync_results['details']['errors']['items']:
            ods_id = error.get('ods_id', 'Unknown')
            message = error.get('message', 'Unknown error')
            
            email_text += f"\n- ODS ID {ods_id}: {message}\n"
    
    if is_error:
        email_text += "\nThe synchronization process did not complete successfully. "
        email_text += "Please check the logs for more details.\n\n"
    else:
        email_text += "\nPlease review the synchronization results in Dataspot.\n\n"
    
    email_text += "Best regards,\n"
    email_text += "Your Dataspot ODS Components Sync Assistant"
    
    return email_subject, email_text, True


if __name__ == '__main__':
    logging.basicConfig(
        level=logging.INFO,
        format='%(levelname)s:%(name)s:[%(filename)s:%(funcName)s:%(lineno)d] %(message)s'
    )
    logging.info(f"=== CURRENT DATABASE: {config.database_name} ===")
    logging.info(f'Executing {__file__}...')
    main()
