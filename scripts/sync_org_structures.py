import logging
import json
import os
import datetime

import config
from src.clients.base_client import BaseDataspotClient
from src.clients.fdm_client import FDMClient
from src.clients.sk_client import SKClient
from src.ods_client import ODSClient
from src.clients.dnk_client import DNKClient
from src.clients.rdm_client import RDMClient
from src.clients.kv_client import KVClient
from src.common import email_helpers as email_helpers


def main():
    dnk_client = DNKClient()
    sync_org_structures(dataspot_client=dnk_client)

    fdm_client = FDMClient()
    sync_org_structures(dataspot_client=fdm_client)

    rdm_client = RDMClient()
    sync_org_structures(dataspot_client=rdm_client)

    kv_client = KVClient()
    sync_org_structures(dataspot_client=kv_client)

    sk_client = SKClient()
    sync_org_structures(dataspot_client=sk_client)

    #tdm_client = TDMClient()

def sync_org_structures(dataspot_client: BaseDataspotClient):
    """
    Synchronize organizational structures (consisting of org units) of the specified dataspot client
    with the latest data from ODS API.

    This method retrieves organization data from the ODS API, validates for duplicate IDs,
    fetches existing organizational units from Dataspot, compares the structures,
    updates only the changed organizations, and provides a summary of changes.

    This method:
    1. Retrieves organization data from the ODS API
    2. Validates that no duplicate id_im_staatskalender values exist in ODS data (throws an error if duplicates are found)
    3. Fetches existing organizational units from Dataspot 
    4. Validates that no duplicate id_im_staatskalender values exist in Dataspot (throws an error if duplicates are found)
    5. Compares with existing organization data in Dataspot
    6. Updates only the changed organizations
    7. Provides a summary of changes
    
    Args:
        dataspot_client: The Dataspot client instance to use for synchronization
        
    Raises:
        ValueError: If duplicate id_im_staatskalender values are detected in either ODS or Dataspot data
        HTTPError: If API requests fail
    """
    logging.info("Starting organization structure synchronization...")

    # Initialize clients
    ods_client = ODSClient()

    # Fetch organization data
    logging.info("Fetching organization data from ODS API...")
    all_organizations = ods_client.get_all_organization_data(batch_size=100)
    logging.info(
        f"Total organizations retrieved: {len(all_organizations['results'])} (out of {all_organizations['total_count']})")

    # Synchronize organization data
    logging.info("Synchronizing organization data with Dataspot...")
    try:
        # Use the sync org units method
        # By default, updates use "WORKING" status (DRAFT group)
        # To automatically publish updates, use status="PUBLISHED"
        # To mark for deletion review, deletions use "REVIEWDCC2" (handled automatically)
        sync_result = dataspot_client.sync_org_units(
            all_organizations, 
            status="PUBLISHED"
        )

        # Get the base URL and database name for asset links
        base_url = dataspot_client.base_url
        database_name = dataspot_client.database_name

        # Display sync results
        logging.info(f"Synchronization {sync_result['status']}!")
        logging.info(f"Message: {sync_result['message']}")

        # Display details if available
        if 'counts' in sync_result:
            counts = sync_result['counts']
            # Get detailed deletion counts
            directly_deleted = counts.get('directly_deleted', 0)
            marked_for_deletion = counts.get('marked_for_deletion', 0)
            
            # Show all details in the log output
            logging.info(f"Changes: {counts['total']} total - {counts['created']} created, "
                        f"{counts['updated']} updated, {counts['deleted']} deleted "
                        f"({directly_deleted} empty directly deleted, "
                        f"{marked_for_deletion} non-empty marked for review)")

        # Show detailed information for each change type - LOG ORDER: creations, updates, deletions
        details = sync_result.get('details', {})
        
        # Fetch UUIDs for newly created organization units
        if 'creations' in details and details['creations'].get('count', 0) > 0:
            created_items = details['creations'].get('items', [])
            created_ids = [str(item['staatskalender_id']) for item in created_items if 'staatskalender_id' in item]
            
            if created_ids:
                logging.info(f"Fetching UUIDs for {len(created_ids)} newly created organizational units...")
                created_units = dataspot_client.get_org_units_by_staatskalender_ids(created_ids)
                
                # Update each creation with its UUID
                for i, item in enumerate(created_items):
                    staatskalender_id = str(item.get('staatskalender_id', ''))
                    if staatskalender_id in created_units:
                        # Add UUID to the sync result
                        details['creations']['items'][i]['uuid'] = created_units[staatskalender_id].get('id')

        # Process creations
        if 'creations' in details:
            creations = details['creations'].get('items', [])
            logging.info(f"\n=== CREATED UNITS ({len(creations)}) ===")
            for i, creation in enumerate(creations, 1):
                title = creation.get('title', '(Unknown)')
                staatskalender_id = creation.get('staatskalender_id', '(Unknown)')
                uuid = creation.get('uuid', '')  # UUID might be missing for newly created items
                
                # Create asset link if UUID is available
                asset_link = f"{base_url}/web/{database_name}/collections/{uuid}" if uuid else "(Link not available)"
                
                # Display in new format with link in the first line
                logging.info(f"{i}. '{title}' (ID: {staatskalender_id}, link: {asset_link})")

                # Show properties
                props = creation.get('properties', {})
                if props:
                    for key, value in props.items():
                        if value:  # Only show non-empty values
                            logging.info(f"   - {key}: '{value}'")

        # Process updates - show field changes with old and new values
        if 'updates' in details:
            updates = details['updates'].get('items', [])
            logging.info(f"\n=== UPDATED UNITS ({len(updates)}) ===")
            for i, update in enumerate(updates, 1):
                title = update.get('title', '(Unknown)')
                staatskalender_id = update.get('staatskalender_id', '(Unknown)')
                uuid = update.get('uuid', '(Unknown)')

                # Create asset link
                asset_link = f"{base_url}/web/{database_name}/collections/{uuid}"

                # Display in new format with link in the first line
                logging.info(f"{i}. '{title}' (ID: {staatskalender_id}, link: {asset_link})")

                # Show each changed field
                for field_name, changes in update.get('changed_fields', {}).items():
                    old_value = changes.get('old_value', '')
                    new_value = changes.get('new_value', '')
                    logging.info(f"   - {field_name}:")
                    logging.info(f"     - Old value: '{old_value}'")
                    logging.info(f"     - New value: '{new_value}'")

        # Process deletions
        if 'deletions' in details:
            deletions = details['deletions'].get('items', [])
            
            # Split deletions into direct deletions and marked for review
            direct_deletions = [d for d in deletions if d.get('is_empty', False)]
            review_deletions = [d for d in deletions if not d.get('is_empty', False)]
            
            # Log information about both types of deletions
            logging.info(f"\n=== DELETED UNITS ({len(deletions)} total) ===")
            
            # Always show both sections for consistency, even if counts are 0
            logging.info(f"--- Directly Deleted Empty Collections ({len(direct_deletions)}) ---")
            for i, deletion in enumerate(direct_deletions, 1):
                title = deletion.get('title', '(Unknown)')
                staatskalender_id = deletion.get('staatskalender_id', '(Unknown)')
                uuid = deletion.get('uuid', '(Unknown)')
                asset_link = f"{base_url}/web/{database_name}/collections/{uuid}"
                logging.info(f"{i}. '{title}' (ID: {staatskalender_id}, link: {asset_link})")
                logging.info(f"   - Path: '{deletion.get('inCollection', '')}'")
            
            logging.info(f"--- Non-Empty Collections Marked for Deletion Review ({len(review_deletions)}) ---")
            for i, deletion in enumerate(review_deletions, 1):
                title = deletion.get('title', '(Unknown)')
                staatskalender_id = deletion.get('staatskalender_id', '(Unknown)')
                uuid = deletion.get('uuid', '(Unknown)')
                asset_link = f"{base_url}/web/{database_name}/collections/{uuid}"
                logging.info(f"{i}. '{title}' (ID: {staatskalender_id}, link: {asset_link})")
                logging.info(f"   - Path: '{deletion.get('inCollection', '')}'")
                
        # Write detailed report to file for email/reference purposes
        report_filename = None
        try:
            # Get project root directory (one level up from src)
            current_file_path = os.path.abspath(__file__)
            project_root = os.path.dirname(os.path.dirname(current_file_path))

            # Define reports directory in project root
            reports_dir = os.path.join(project_root, "reports")

            # Create reports directory if it doesn't exist
            os.makedirs(reports_dir, exist_ok=True)

            # Generate filename with timestamp
            timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
            report_filename = os.path.join(reports_dir, f"org_sync_report_{timestamp}.json")

            # Write report to file
            with open(report_filename, 'w', encoding='utf-8') as f:
                json.dump(sync_result, f, indent=2, ensure_ascii=False)

            logging.info(f"\nDetailed report saved to {report_filename}")
        except Exception as e:
            logging.error(f"Failed to save detailed report to file: {str(e)}")

        # Create email content using the new function
        email_subject, email_content, should_send = create_email_content(
            sync_result=sync_result,
            base_url=base_url,
            database_name=database_name,
            scheme_name_short=dataspot_client.scheme_name_short
        )

        # Send email if there were changes
        if should_send:
            # Create and send email
            attachment = report_filename if report_filename and os.path.exists(report_filename) else None
            msg = email_helpers.create_email_msg(
                subject=email_subject,
                text=email_content,
                attachment=attachment
            )
            email_helpers.send_email(msg)
            logging.info("Email notification sent successfully")
        else:
            logging.info("No changes detected - email notification not sent")

    except ValueError as e:
        if "Duplicate id_im_staatskalender values detected in Dataspot" in str(e):
            logging.error("============================================================")
            logging.error("ERROR: SYNCHRONIZATION ABORTED - DUPLICATE IDs IN DATASPOT")
            logging.error("------------------------------------------------------------")
            logging.error(str(e))
            logging.error("------------------------------------------------------------")
            logging.error("Please fix the duplicate IDs in Dataspot before continuing.")
            logging.error("You may need to manually delete one of the duplicate collections.")
            logging.error("============================================================")
            exit()
        elif "Duplicate id_im_staatskalender values detected" in str(e):
            logging.error("============================================================")
            logging.error("ERROR: SYNCHRONIZATION ABORTED - DUPLICATE IDs IN ODS DATA")
            logging.error("------------------------------------------------------------")
            logging.error(str(e))
            logging.error("------------------------------------------------------------")
            logging.error("Please fix the duplicate IDs in the ODS source data before continuing.")
            logging.error("============================================================")
            exit()
        else:
            # Re-raise other ValueError exceptions
            raise
    except Exception as e:
        logging.error(f"Error synchronizing organization structure: {str(e)}")

    logging.info("Organization structure synchronization process finished")
    logging.info("===============================================")

def create_email_content(sync_result, base_url, database_name, scheme_name_short) -> (str | None, str | None, bool):
    """
    Create email content based on synchronization results.

    Args:
        sync_result (dict): Synchronization result data
        base_url (str): Base URL for asset links
        database_name (str): Database name for asset links
        scheme_name_short (str): Short name of the scheme

    Returns:
        tuple: (email_subject, email_text, should_send)
    """
    counts = sync_result.get('counts', {})
    total_changes = counts.get('total', 0)
    details = sync_result.get('details', {})

    # Only create email if there were changes
    if total_changes == 0:
        return None, None, False

    # Get more detailed deletion counts if available
    directly_deleted = counts.get('directly_deleted', 0)
    marked_for_deletion = counts.get('marked_for_deletion', 0)
    
    # Create email subject with summary of changes
    email_subject = f"[{database_name}/{scheme_name_short}] Org Structure: {counts.get('created', 0)} created, {counts.get('updated', 0)} updated, {counts.get('deleted', 0)} deleted"

    email_text = f"Hi there,\n\n"
    email_text += f"I've just updated the organization structure in Dataspot.\n"
    email_text += f"Please review the changes below. No action is needed if everything looks correct.\n\n"

    email_text += f"Here's what changed:\n"
    email_text += f"- Total: {counts.get('total', 0)} changes\n"
    email_text += f"- Created: {counts.get('created', 0)} organizational units\n"
    email_text += f"- Updated: {counts.get('updated', 0)} organizational units\n"
    email_text += f"- Deleted: {counts.get('deleted', 0)} organizational units\n"
    email_text += f"  * {marked_for_deletion} non-empty collections marked for deletion review\n"
    email_text += f"  * {directly_deleted} empty collections directly deleted\n"
    
    email_text += "\n"

    # Add details about each change type - EMAIL ORDER: deletions, updates, creations
    if counts.get('deleted', 0) > 0 and 'deletions' in details:
        deletions = details['deletions'].get('items', [])
        
        # Split deletions into direct deletions and marked for review
        direct_deletions = [d for d in deletions if d.get('is_empty', False)]
        review_deletions = [d for d in deletions if not d.get('is_empty', False)]
        
        # Update email content with more detailed information
        email_text += f"Deleted organizational units ({len(deletions)} total):\n"
        
        # Show marked for review first
        email_text += f"- Non-Empty Collections Marked for Deletion Review ({len(review_deletions)}):\n"
        for deletion in review_deletions:
            title = deletion.get('title', '(Unknown)')
            staatskalender_id = deletion.get('staatskalender_id', '(Unknown)')
            uuid = deletion.get('uuid', '(Unknown)')
            asset_link = f"{base_url}/web/{database_name}/collections/{uuid}"
            email_text += f"  * {title} (ID: {staatskalender_id}, link: {asset_link})\n"
            email_text += f"    Path: '{deletion.get('inCollection', '')}'\n"
        
        # Then show directly deleted (without links)
        email_text += f"- Directly Deleted Empty Collections ({len(direct_deletions)}):\n"
        for deletion in direct_deletions:
            title = deletion.get('title', '(Unknown)')
            staatskalender_id = deletion.get('staatskalender_id', '(Unknown)')
            email_text += f"  * {title} (ID: {staatskalender_id})\n"
            email_text += f"    Path: '{deletion.get('inCollection', '')}'\n"
        
        email_text += "\n"

    if counts.get('updated', 0) > 0 and 'updates' in details:
        updates = details['updates'].get('items', [])
        email_text += f"Updated organizational units ({len(updates)}):\n"
        for update in updates:
            title = update.get('title', '(Unknown)')
            staatskalender_id = update.get('staatskalender_id', '(Unknown)')
            uuid = update.get('uuid', '(Unknown)')
            asset_link = f"{base_url}/web/{database_name}/collections/{uuid}"
            email_text += f"- {title} (ID: {staatskalender_id}, link: {asset_link})\n"
            for field_name, changes in update.get('changed_fields', {}).items():
                old_value = changes.get('old_value', '')
                new_value = changes.get('new_value', '')
                email_text += f"  {field_name}:\n"
                email_text += f"    - Old value: '{old_value}'\n"
                email_text += f"    - New value: '{new_value}'\n"
        email_text += "\n"

    if counts.get('created', 0) > 0 and 'creations' in details:
        creations = details['creations'].get('items', [])
        email_text += f"New organizational units ({len(creations)}):\n"
        for creation in creations:
            title = creation.get('title', '(Unknown)')
            staatskalender_id = creation.get('staatskalender_id', '(Unknown)')
            uuid = creation.get('uuid', '')
            asset_link = f"{base_url}/web/{database_name}/collections/{uuid}" if uuid else "Link not available"
            email_text += f"- {title} (ID: {staatskalender_id}, link: {asset_link})\n"
            props = creation.get('properties', {})
            if props:
                for key, value in props.items():
                    if value:
                        email_text += f"  {key}: '{value}'\n"
        email_text += "\n"

    email_text += "Best regards,\n"
    email_text += "Your Dataspot Organization Structure Sync Assistant"

    return email_subject, email_text, True

if __name__ == '__main__':
    logging.basicConfig(
        level=logging.INFO,
        format='%(levelname)s:%(name)s:[%(filename)s:%(funcName)s:%(lineno)d] %(message)s'
    )
    logging.info(f"=== CURRENT DATABASE: {config.database_name} ===")
    logging.info(f'Executing {__file__}...')
    main()
