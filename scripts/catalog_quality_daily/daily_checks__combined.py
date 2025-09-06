import logging
import json
import os
import datetime
import config
from src.common import email_helpers
from src.clients.base_client import BaseDataspotClient


def main():
    """
    Main function to run all daily checks and generate a combined report.
    """
    logging.info("")
    logging.info("-----[ DATASPOT DAILY CHECKS STARTING ]" + "-" * 35)
    logging.info("")
    
    # Run all checks and collect results
    check_results = run_all_checks()

    # Generate a combined report from all check results
    combined_report = aggregate_results(check_results)

    # Write combined report to file
    write_combined_report(combined_report)

    # Log combined results
    log_combined_results(combined_report)

    # Send combined email notification if needed
    send_combined_email(combined_report)


def run_all_checks():
    """
    Run all available checks and return their results.
    
    Returns:
        list: List of check result dictionaries
    """
    check_results = []

    # Initialize client for SQL calls
    dataspot_base_client = BaseDataspotClient(base_url=config.base_url, database_name=config.database_name,
                                         scheme_name='NOT_IN_USE', scheme_name_short='NotFound404')

    run_check_1 = True
    run_check_2 = True
    run_check_3 = True
    run_check_4 = True
    run_check_5 = True

    if run_check_1:
        # Check #1: Unique sk_person_id verification
        logging.info("")
        logging.info("   Starting Check #1: Unique sk_person_id Verification...")
        logging.info("   " + "-" * 50)
        from scripts.catalog_quality_daily.check_1_unique_sk_person_id import check_1_unique_sk_person_id
        result = check_1_unique_sk_person_id(dataspot_client=dataspot_base_client)
        check_results.append({
            'check_name': 'check_1_unique_sk_person_id',
            'title': 'Check #1: Unique sk_person_id Verification',
            'description': 'Checks if all persons have unique sk_person_id values.',
            'results': result
        })
        logging.info("")
        logging.info("   Check #1: Unique sk_person_id Verification Completed.")
        logging.info("")

    # TODO: Refactor to staatskalender_post_person_mapping
    post_person_mapping__should = []
    staatskalender_person_email_cache = {}

    if run_check_2:
        # Check #2: Person assignment according to Staatskalender
        logging.info("")
        logging.info("   Starting Check #2: Person Assignment (Staatskalender)...")
        logging.info("   " + "-" * 50)
        from scripts.catalog_quality_daily.check_2_staatskalender_assignment import check_2_staatskalender_assignment
        check_2_result = check_2_staatskalender_assignment(dataspot_client=dataspot_base_client)
        check_results.append({
            'check_name': 'check_2_staatskalender_assignment',
            'title': 'Check #2: Person Assignment (Staatskalender)',
            'description': 'Checks if all posts with membership IDs have the correct person assignments from Staatskalender.',
            'results': check_2_result
        })
        post_person_mapping__should = check_2_result['post_person_mapping__should']
        staatskalender_person_email_cache = check_2_result.get('staatskalender_person_email_cache', {})

        logging.info("")
        logging.info("   Check #2: Person Assignment (Staatskalender) Completed.")
        logging.info("")

    if run_check_3:
        # Check #3: Membership-based Post Assignments
        logging.info("")
        logging.info("   Starting Check #3: Membership-based Post Assignments...")
        logging.info("   " + "-" * 50)
        from scripts.catalog_quality_daily.check_3_post_assignment import check_3_post_assignment

        logging.debug(f"   Using post_person_mapping__should from check_2 with {len(post_person_mapping__should)} mappings")

        result = check_3_post_assignment(
            dataspot_client=dataspot_base_client,
            post_person_mapping__should=post_person_mapping__should
        )
        check_results.append({
            'check_name': 'check_3_post_assignment',
            'title': 'Check #3: Membership-based Post Assignments',
            'description': 'Checks if all posts with membership IDs have correct person assignments from Staatskalender.',
            'results': result
        })
        logging.info("")
        logging.info("   Check #3: Membership-based Post Assignments Completed.")
        logging.info("")

    if run_check_4:
        # Check #4: Post occupation check
        logging.info("")
        logging.info("   Starting Check #4: Post Occupation...")
        logging.info("   " + "-" * 50)
        from scripts.catalog_quality_daily.check_4_post_occupation import check_4_post_occupation
        result = check_4_post_occupation(dataspot_client=dataspot_base_client)
        check_results.append({
            'check_name': 'check_4_post_occupation',
            'title': 'Check #4: Post Occupation',
            'description': 'Checks if all posts are assigned to at least one person.',
            'results': result
        })
        logging.info("")
        logging.info("   Check #4: Post Occupation Completed.")
        logging.info("")

    if run_check_5:
        # Check #5: User assignment for persons with sk_person_id
        logging.info("")
        logging.info("   Starting Check #5: User Assignment...")
        logging.info("   " + "-" * 50)
        from scripts.catalog_quality_daily.check_5_user_assignment import check_5_user_assignment
        
        logging.debug(f"   Using staatskalender_person_email_cache from check_2 with {len(staatskalender_person_email_cache)} email mappings")
        
        result = check_5_user_assignment(
            dataspot_client=dataspot_base_client,
            staatskalender_person_email_cache=staatskalender_person_email_cache
        )
        check_results.append({
            'check_name': 'check_5_user_assignment',
            'title': 'Check #5: User Assignment for Persons',
            'description': 'Checks if all persons with sk_person_id have the correct user assignments.',
            'results': result
        })
        logging.info("")
        logging.info("   Check #5: User Assignment Completed.")
        logging.info("")

    
    logging.info("")
    logging.info("-----[ All Checks Completed ]" + "-" * 45)
    logging.info("")

    return check_results


def aggregate_results(check_results):
    """
    Aggregate results from multiple checks into a single report.
    
    Args:
        check_results (list): List of check result dictionaries
    
    Returns:
        dict: Combined report
    """
    # Initialize combined report structure
    combined_report = {
        'timestamp': datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        'database_name': config.database_name,
        'summary': {
            'total_checks': len(check_results),
            'successful': 0,
            'warnings': 0,
            'errors': 0,
            'total_issues': 0,
            'remediated_issues': 0,  # Count of issues that were automatically fixed
            'actual_issues': 0,      # Count of issues that need manual intervention
            'overall_status': 'success'  # Will be downgraded as needed
        },
        'checks': []
    }

    # Process each check result
    for check in check_results:
        check_name = check.get('check_name')
        title = check.get('title')
        description = check.get('description')
        results = check.get('results', {})

        # Extract key information
        status = results.get('status', 'unknown')
        message = results.get('message', '')
        issues = results.get('issues', [])
        error = results.get('error')

        # Count and categorize issues
        remediated_issues = []
        actual_issues = []
        
        for issue in issues:
            # Check if this issue was successfully remediated
            if issue.get('remediation_attempted', False) and issue.get('remediation_success', False):
                remediated_issues.append(issue)
            else:
                actual_issues.append(issue)
        
        # Determine the actual status based on unresolved issues
        actual_status = 'success'
        if len(actual_issues) > 0:
            if error or status == 'error':
                actual_status = 'error'
            else:
                actual_status = 'warning'
        
        # Update summary counters
        if actual_status == 'success':
            combined_report['summary']['successful'] += 1
        elif actual_status == 'warning':
            combined_report['summary']['warnings'] += 1
            # Downgrade overall status if currently successful
            if combined_report['summary']['overall_status'] == 'success':
                combined_report['summary']['overall_status'] = 'warning'
        elif actual_status == 'error':
            combined_report['summary']['errors'] += 1
            # Always downgrade to error if any check has an error
            combined_report['summary']['overall_status'] = 'error'

        # Update the summary counts
        combined_report['summary']['total_issues'] += len(issues)
        combined_report['summary']['remediated_issues'] += len(remediated_issues)
        combined_report['summary']['actual_issues'] += len(actual_issues)

        # Add check details to combined report
        combined_report['checks'].append({
            'name': check_name,
            'title': title,
            'description': description,
            'status': status,
            'message': message,
            'issues_count': len(issues),
            'remediated_issues_count': len(remediated_issues),
            'actual_issues_count': len(actual_issues),
            'remediated_issues': remediated_issues,
            'actual_issues': actual_issues,
            'issues': issues,  # Keep the original issues list for backward compatibility
            'error': error
        })

    return combined_report


def write_combined_report(combined_report):
    """
    Write combined check results to a JSON file.
    
    Args:
        combined_report (dict): The combined report
    """
    report_file = get_combined_report_file_path()

    try:
        # Create reports directory if it doesn't exist
        os.makedirs(os.path.dirname(report_file), exist_ok=True)

        # Write report to file
        with open(report_file, 'w', encoding='utf-8') as f:
            json.dump(combined_report, f, indent=2, ensure_ascii=False)
        logging.info(f"Detailed combined report saved to {report_file}")
    except Exception as report_error:
        logging.error(f"Failed to write combined report file: {str(report_error)}")


def get_combined_report_file_path():
    """
    Generate the path for the combined report file.
    
    Returns:
        str: The path to the report file
    """
    # Get project root directory (two levels up from catalog_quality_daily)
    current_file_path = os.path.abspath(__file__)
    project_root = os.path.dirname(os.path.dirname(os.path.dirname(current_file_path)))

    # Define reports directory in project root
    reports_dir = os.path.join(project_root, "reports")

    # Generate filename with timestamp
    timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
    return os.path.join(reports_dir, f"dataspot_daily_checks_{timestamp}.json")


def log_combined_results(combined_report):
    """
    Log a detailed report of the combined check results.
    
    Args:
        combined_report (dict): The combined report
    """
    logging.info("")
    logging.info("--------[ DATASPOT DAILY CHECKS SUMMARY REPORT ]" + "-" * 21)
    logging.info("")

    # Log summary
    summary = combined_report.get('summary', {})
    overall_status = summary.get('overall_status', 'unknown').upper()
    total_checks = summary.get('total_checks', 0)
    successful = summary.get('successful', 0)
    warnings = summary.get('warnings', 0)
    errors = summary.get('errors', 0)
    total_issues = summary.get('total_issues', 0)

    logging.info(f" Overall Status: {overall_status}")
    logging.info(f" Database: {combined_report.get('database_name')}")
    logging.info(f" Time: {combined_report.get('timestamp')}")
    logging.info(f" Checks: {total_checks} total - {successful} successful, {warnings} with warnings, {errors} with errors")
    
    # Extract counts of remediated and actual issues
    remediated_issues = summary.get('remediated_issues', 0)
    actual_issues = summary.get('actual_issues', 0)
    
    logging.info(f" Issues: {total_issues} total ({remediated_issues} automatically fixed, {actual_issues} requiring attention)")

    # Log details for each check
    for check in combined_report.get('checks', []):
        check_name = check.get('name', 'Unknown')
        title = check.get('title', 'Unknown Check')
        status = check.get('status', 'unknown').upper()
        issues_count = check.get('issues_count', 0)

        logging.info("")
        logging.info(f"-  {title} ({status})")
        logging.info(f"   Message: {check.get('message', 'No message')}")

        # Log issues for checks with issues
        if issues_count > 0:
            remediated_count = check.get('remediated_issues_count', 0)
            actual_count = check.get('actual_issues_count', 0)
            logging.info(f"   Issues: {issues_count} total ({remediated_count} automatically fixed, {actual_count} requiring attention)")

            # Group remediated issues by type
            if remediated_count > 0:
                remediated_issues = check.get('remediated_issues', [])
                logging.info("")
                logging.info(f"   === AUTOMATICALLY FIXED ISSUES ({remediated_count}) ===")
                
                # Group remediated issues by type for easier reading
                remediated_by_type = {}
                for issue in remediated_issues:
                    issue_type = issue.get('type', 'unknown')
                    if issue_type not in remediated_by_type:
                        remediated_by_type[issue_type] = []
                    remediated_by_type[issue_type].append(issue)
                
                # Log each remediated issue type
                for issue_type, issues in remediated_by_type.items():
                    logging.info("")
                    if issue_type == 'person_mismatch_missing_email':
                        logging.info(f"   * Person Missing Email In Staatskalender ({len(issues)})")
                    elif issue_type == 'person_without_user':
                        logging.info(f"   * Person Without User Account ({len(issues)})")
                    elif issue_type == 'user_created':
                        logging.info(f"   * User Account Created Successfully ({len(issues)})")
                    elif issue_type == 'user_creation_failed':
                        logging.info(f"   * User Account Creation Failed ({len(issues)})")
                    elif issue_type == 'person_name_update':
                        logging.info(f"   * Person Name Updated From Staatskalender ({len(issues)})")
                    elif issue_type == 'person_name_update_failed':
                        logging.info(f"   * Person Name Update Failed ({len(issues)})")
                    elif issue_type == 'access_level_updated':
                        logging.info(f"   * User Access Level Updated ({len(issues)})")
                    elif issue_type == 'access_level_update_failed':
                        logging.info(f"   * User Access Level Update Failed ({len(issues)})")
                    elif issue_type == 'user_person_link_updated':
                        logging.info(f"   * User Linked To Person Successfully ({len(issues)})")
                    elif issue_type == 'user_person_link_update_failed':
                        logging.info(f"   * User To Person Link Failed ({len(issues)})")
                    else:
                        logging.info(f"   * {issue_type.replace('_', ' ').title()} ({len(issues)})")
                    
                    # List the fixed issues
                    for issue in issues:

                        if issue.get('person_uuid'):
                            person_uuid = issue.get('person_uuid')
                            first_name = issue.get('sk_first_name')
                            last_name = issue.get('sk_last_name')
                            message = issue.get('message', 'No message')

                            person_name = "Unknown"
                            if first_name and last_name:
                                person_name = f"{first_name} {last_name}"


                            logging.info(f"     - {person_name}")
                            logging.info(f"       URL: https://datenkatalog.bs.ch/web/{combined_report.get('database_name')}/persons/{person_uuid}")
                            logging.info(f"       Message: {message}")
                        else:
                            post_label = issue.get('post_label', 'Unknown')
                            post_uuid = issue.get('post_uuid', 'Unknown')
                            message = issue.get('message', 'No message')

                            logging.info(f"     - {post_label}")
                            logging.info(f"       URL: https://datenkatalog.bs.ch/web/{combined_report.get('database_name')}/posts/{post_uuid}")
                            logging.info(f"       Message: {message}")
            
            # Group actual issues by type
            if actual_count > 0:
                actual_issues = check.get('actual_issues', [])
                logging.info("")
                logging.info(f"   === ISSUES REQUIRING ATTENTION ({actual_count}) ===")
                
                # Group actual issues by type for easier reading
                actual_by_type = {}
                for issue in actual_issues:
                    issue_type = issue.get('type', 'unknown')
                    if issue_type not in actual_by_type:
                        actual_by_type[issue_type] = []
                    actual_by_type[issue_type].append(issue)
                
                # Log each actual issue type
                for issue_type, issues in actual_by_type.items():
                    logging.info("")
                    if issue_type == 'person_mismatch_missing_email':
                        logging.info(f"   * Person Missing Email In Staatskalender ({len(issues)})")
                    elif issue_type == 'person_without_user':
                        logging.info(f"   * Person Without User Account ({len(issues)})")
                    elif issue_type == 'user_created':
                        logging.info(f"   * User Account Created Successfully ({len(issues)})")
                    elif issue_type == 'user_creation_failed':
                        logging.info(f"   * User Account Creation Failed ({len(issues)})")
                    elif issue_type == 'person_name_update':
                        logging.info(f"   * Person Name Updated From Staatskalender ({len(issues)})")
                    elif issue_type == 'person_name_update_failed':
                        logging.info(f"   * Person Name Update Failed ({len(issues)})")
                    elif issue_type == 'access_level_updated':
                        logging.info(f"   * User Access Level Updated ({len(issues)})")
                    elif issue_type == 'access_level_update_failed':
                        logging.info(f"   * User Access Level Update Failed ({len(issues)})")
                    elif issue_type == 'user_person_link_updated':
                        logging.info(f"   * User Linked To Person Successfully ({len(issues)})")
                    elif issue_type == 'user_person_link_update_failed':
                        logging.info(f"   * User To Person Link Failed ({len(issues)})")
                    else:
                        logging.info(f"   * {issue_type.replace('_', ' ').title()} ({len(issues)})")
                    
                    # List the issues requiring attention
                    for issue in issues:
                        message = issue.get('message', 'No message')
                        
                        # Format differently based on issue type
                        if issue_type == 'person_without_user':
                            person_uuid = issue.get('person_uuid', 'Unknown')
                            given_name = issue.get('given_name', '')
                            family_name = issue.get('family_name', '')
                            person_name = f"{given_name} {family_name}"
                            posts_count = issue.get('posts_count', 0)
                            
                            logging.info(f"     - Person: {person_name} (ID: {person_uuid})")
                            if person_uuid != 'Unknown':
                                logging.info(f"       URL: https://datenkatalog.bs.ch/web/{combined_report.get('database_name')}/persons/{person_uuid}")
                            logging.info(f"       Posts count: {posts_count}")
                            logging.info(f"       Message: {message}")
                        elif issue_type == 'duplicate_sk_person_id':
                            sk_person_id = issue.get('sk_person_id', 'Unknown')
                            person_names = issue.get('person_names', [])
                            
                            logging.info(f"     - Duplicate sk_person_id: {sk_person_id}")
                            logging.info(f"       Affected persons: {', '.join(person_names)}")
                            logging.info(f"       Message: {message}")
                        elif issue_type in ['person_mismatch_missing_email', 'person_name_update', 'person_name_update_failed', 
                             'access_level_updated', 'access_level_update_failed', 'user_person_link_updated', 'user_person_link_update_failed']:
                            person_uuid = issue.get('person_uuid', 'Unknown')
                            given_name = issue.get('given_name', '')
                            family_name = issue.get('family_name', '')
                            person_name = f"{given_name} {family_name}"
                            
                            logging.info(f"     - Person: {person_name} (ID: {person_uuid})")
                            if person_uuid != 'Unknown':
                                logging.info(f"       URL: https://datenkatalog.bs.ch/web/{combined_report.get('database_name')}/persons/{person_uuid}")
                                
                            # Add SK name details for name updates
                            if issue_type in ['person_name_update', 'person_name_update_failed']:
                                sk_first_name = issue.get('sk_first_name', '')
                                sk_last_name = issue.get('sk_last_name', '')
                                if sk_first_name and sk_last_name:
                                    sk_name = f"{sk_first_name} {sk_last_name}"
                                    logging.info(f"       Staatskalender name: {sk_name}")
                            # Add user access level details
                            elif issue_type in ['access_level_updated', 'access_level_update_failed']:
                                user_email = issue.get('user_email', '')
                                if user_email:
                                    logging.info(f"       User email: {user_email}")
                                old_level = issue.get('user_access_level_old', issue.get('user_access_level', 'Unknown'))
                                logging.info(f"       Previous access level: {old_level}")
                                if issue_type == 'access_level_updated':
                                    new_level = issue.get('user_access_level_new', ['Unknown'])[0]
                                    logging.info(f"       New access level: {new_level}")
                            # Add user link details
                            elif issue_type in ['user_person_link_updated', 'user_person_link_update_failed']:
                                user_email = issue.get('user_email', '')
                                if user_email:
                                    logging.info(f"       User email: {user_email}")
                                logging.info(f"       Person: {person_name}")
                            
                            logging.info(f"       Message: {message}")
                        elif issue_type == 'user_created':
                            user_email = issue.get('user_email', '')
                            logging.info(f"       User email: {user_email}")
                            logging.info(f"       Message: {message}")
                        elif issue_type == 'user_creation_failed':
                            user_email = issue.get('user_email', '')
                            logging.info(f"       User email: {user_email}")
                            logging.info(f"       Message: {message}")
                        else:
                            logging.warning(f"Unknown issue type: {issue_type}")
                            post_label = issue.get('post_label', 'Unknown')
                            post_uuid = issue.get('post_uuid', 'Unknown')
                            
                            logging.info(f"     - {post_label}")
                            if post_uuid != 'Unknown':
                                logging.info(f"       URL: https://datenkatalog.bs.ch/web/{combined_report.get('database_name')}/posts/{post_uuid}")
                            logging.info(f"       Message: {message}")

        # Log error if any
        if check.get('error'):
            logging.info("")
            logging.info("   ERROR DETAILS:")
            logging.info(f"     {check.get('error')}")

        logging.info("")
        logging.info("-" * 78)
        logging.info("")


def send_combined_email(combined_report):
    """
    Send a combined email report based on all check results.
    
    Args:
        combined_report (dict): The combined report
    """
    # Get summary information
    summary = combined_report.get('summary', {})
    overall_status = summary.get('overall_status', 'unknown')
    total_issues = summary.get('total_issues', 0)

    # Only send email if there are issues or errors
    if overall_status == 'success' and total_issues == 0:
        logging.info("All checks passed, no email notification needed")
        return

    # Create email subject based on overall status
    database_name = combined_report.get('database_name', 'unknown')

    if overall_status == 'error':
        email_subject = f"[ERROR][{database_name}] Dataspot Daily Checks Failed"
    elif overall_status == 'warning':
        email_subject = f"[{database_name}] Dataspot Daily Checks: {total_issues} Issues Found"
    else:
        email_subject = f"[INFO][{database_name}] Dataspot Daily Checks Report"

    # Begin building email content
    email_text = f"Hi there,\n\n"
    email_text += f"I've just completed the daily checks for {database_name}.\n\n"

    # Add summary section
    email_text += "=== SUMMARY ===\n"
    email_text += f"Time: {combined_report.get('timestamp')}\n"
    email_text += f"Overall Status: {overall_status.upper()}\n"
    email_text += f"Checks: {summary.get('total_checks', 0)} total - "
    email_text += f"{summary.get('successful', 0)} successful, "
    email_text += f"{summary.get('warnings', 0)} with warnings, "
    email_text += f"{summary.get('errors', 0)} with errors\n"
    
    # Get counts of remediated and actual issues
    remediated_issues_count = summary.get('remediated_issues', 0)
    actual_issues_count = summary.get('actual_issues', 0)
    
    email_text += f"Issues: {total_issues} total\n"
    if remediated_issues_count > 0:
        email_text += f"- {remediated_issues_count} automatically fixed\n"
    if actual_issues_count > 0:
        email_text += f"- {actual_issues_count} requiring attention\n\n"
    else:
        email_text += "- All issues were automatically fixed!\n\n"

    # Add details for each check
    email_text += "=== CHECK RESULTS ===\n"

    for check in combined_report.get('checks', []):
        check_name = check.get('name', 'Unknown')
        title = check.get('title', 'Unknown Check')
        status = check.get('status', 'unknown').upper()
        issues_count = check.get('issues_count', 0)

        email_text += f"\n--- {title} ({status}) ---\n"
        email_text += f"Message: {check.get('message', 'No message')}\n"

        # Add issues for checks with issues
        if issues_count > 0:
            remediated_count = check.get('remediated_issues_count', 0)
            actual_count = check.get('actual_issues_count', 0)
            email_text += f"Issues: {issues_count} total ({remediated_count} automatically fixed, {actual_count} requiring attention)\n"

            # Priority order for sorting issue types
            priority_order = [
                'user_created', 'user_creation_failed', 'person_without_user', 'person_mismatch_missing_email', 'person_mismatch',
                'user_person_link_updated', 'user_person_link_update_failed', 'access_level_updated', 'access_level_update_failed',
                'no_person_assigned', 'unoccupied_post', 'missing_membership', 'invalid_membership', 
                'missing_person_link', 'missing_person_name', 'missing_dataspot_name', 
                'dataspot_person_error', 'processing_error'
            ]

            # Add automatically fixed issues section
            if remediated_count > 0:
                remediated_issues = check.get('remediated_issues', [])
                email_text += f"\n=== AUTOMATICALLY FIXED ISSUES ({remediated_count}) ===\n"
                
                # Group remediated issues by type for easier reading
                remediated_by_type = {}
                for issue in remediated_issues:
                    issue_type = issue.get('type', 'unknown')
                    if issue_type not in remediated_by_type:
                        remediated_by_type[issue_type] = []
                    remediated_by_type[issue_type].append(issue)
                
                # Sort issue types by priority
                sorted_remediated_types = sorted(
                    remediated_by_type.keys(),
                    key=lambda x: priority_order.index(x) if x in priority_order else 999
                )
                
                # Add all remediated issues by type
                for issue_type in sorted_remediated_types:
                    issues = remediated_by_type[issue_type]
                    if issue_type == 'person_mismatch_missing_email':
                        email_text += f"\nPERSON MISSING EMAIL IN STAATSKALENDER ({len(issues)}):\n"
                    elif issue_type == 'person_without_user':
                        email_text += f"\nPERSON WITHOUT USER ACCOUNT ({len(issues)}):\n"
                    elif issue_type == 'person_name_update':
                        email_text += f"\nPERSON NAME UPDATED FROM STAATSKALENDER ({len(issues)}):\n"
                    elif issue_type == 'person_name_update_failed':
                        email_text += f"\nPERSON NAME UPDATE FAILED ({len(issues)}):\n"
                    elif issue_type == 'access_level_updated':
                        email_text += f"\nUSER ACCESS LEVEL UPDATED ({len(issues)}):\n"
                    elif issue_type == 'access_level_update_failed':
                        email_text += f"\nUSER ACCESS LEVEL UPDATE FAILED ({len(issues)}):\n"
                    elif issue_type == 'user_person_link_updated':
                        email_text += f"\nUSER LINKED TO PERSON SUCCESSFULLY ({len(issues)}):\n"
                    elif issue_type == 'user_person_link_update_failed':
                        email_text += f"\nUSER TO PERSON LINK FAILED ({len(issues)}):\n"
                    else:
                        email_text += f"\n{issue_type.replace('_', ' ').upper()} ISSUES ({len(issues)}):\n"
                    
                    # Show all remediated issues
                    for idx, issue in enumerate(issues):
                        post_label = issue.get('post_label', 'Unknown')
                        message = issue.get('message', 'No message provided')
                        
                        email_text += f"\n- {post_label}\n"
                        
                        # Handle person-related issues differently
                        if issue_type in ['person_mismatch_missing_email', 'person_without_user', 'person_name_mismatch']:
                            person_uuid = issue.get('person_uuid', 'Unknown')
                            if person_uuid != 'Unknown':
                                email_text += f"  URL: https://datenkatalog.bs.ch/web/{database_name}/persons/{person_uuid}\n"
                        else:
                            post_uuid = issue.get('post_uuid', 'Unknown')
                            if post_uuid != 'Unknown':
                                email_text += f"  URL: https://datenkatalog.bs.ch/web/{database_name}/posts/{post_uuid}\n"
                        
                        # Add specific details based on issue type
                        if issue_type == 'person_mismatch':
                            sk_name = f"{issue.get('sk_first_name', '')} {issue.get('sk_last_name', '')}"
                            ds_name = f"{issue.get('dataspot_first_name', '')} {issue.get('dataspot_last_name', '')}"
                            email_text += f"  Reassigned from: {ds_name} to: {sk_name}\n"
                        
                        email_text += f"  Resolution: {message}\n"

            # Add issues requiring attention section
            if actual_count > 0:
                actual_issues = check.get('actual_issues', [])
                email_text += f"\n=== ISSUES REQUIRING ATTENTION ({actual_count}) ===\n"
                
                # Group actual issues by type for easier reading
                actual_by_type = {}
                for issue in actual_issues:
                    issue_type = issue.get('type', 'unknown')
                    if issue_type not in actual_by_type:
                        actual_by_type[issue_type] = []
                    actual_by_type[issue_type].append(issue)
                
                # Sort issue types by priority
                sorted_actual_types = sorted(
                    actual_by_type.keys(),
                    key=lambda x: priority_order.index(x) if x in priority_order else 999
                )
                
                # Add all actual issues by type
                for issue_type in sorted_actual_types:
                    issues = actual_by_type[issue_type]
                    if issue_type == 'person_mismatch_missing_email':
                        email_text += f"\nPERSON MISSING EMAIL IN STAATSKALENDER ({len(issues)}):\n"
                    elif issue_type == 'person_without_user':
                        email_text += f"\nPERSON WITHOUT USER ACCOUNT ({len(issues)}):\n"
                    elif issue_type == 'person_name_update':
                        email_text += f"\nPERSON NAME UPDATED FROM STAATSKALENDER ({len(issues)}):\n"
                    elif issue_type == 'person_name_update_failed':
                        email_text += f"\nPERSON NAME UPDATE FAILED ({len(issues)}):\n"
                    elif issue_type == 'access_level_updated':
                        email_text += f"\nUSER ACCESS LEVEL UPDATED ({len(issues)}):\n"
                    elif issue_type == 'access_level_update_failed':
                        email_text += f"\nUSER ACCESS LEVEL UPDATE FAILED ({len(issues)}):\n"
                    elif issue_type == 'user_person_link_updated':
                        email_text += f"\nUSER LINKED TO PERSON SUCCESSFULLY ({len(issues)}):\n"
                    elif issue_type == 'user_person_link_update_failed':
                        email_text += f"\nUSER TO PERSON LINK FAILED ({len(issues)}):\n"
                    else:
                        email_text += f"\n{issue_type.replace('_', ' ').upper()} ISSUES ({len(issues)}):\n"
                    
                    # Show all actual issues
                    for idx, issue in enumerate(issues):
                        message = issue.get('message', 'No message provided')
                        
                        # Format differently for person-related issues
                        if issue_type == 'person_without_user':
                            person_uuid = issue.get('person_uuid', 'Unknown')
                            given_name = issue.get('given_name', '')
                            family_name = issue.get('family_name', '')
                            person_name = f"{given_name} {family_name}"
                            email_text += f"\n- Person: {person_name} (ID: {person_uuid})\n"
                            email_text += f"  URL: https://datenkatalog.bs.ch/web/{database_name}/persons/{person_uuid}\n"
                        elif issue_type == 'person_mismatch_missing_email':
                            person_uuid = issue.get('person_uuid', 'Unknown')
                            given_name = issue.get('given_name', '')
                            family_name = issue.get('family_name', '')
                            person_name = f"{given_name} {family_name}"
                            email_text += f"\n- Person: {person_name}\n"
                            if person_uuid != 'Unknown':
                                email_text += f"  URL: https://datenkatalog.bs.ch/web/{database_name}/persons/{person_uuid}\n"
                        elif issue_type == 'duplicate_sk_person_id':
                            sk_person_id = issue.get('sk_person_id', 'Unknown')
                            person_names = issue.get('person_names', [])
                            email_text += f"\n- Duplicate sk_person_id: {sk_person_id}\n"
                            email_text += f"  Affected persons: {', '.join(person_names)}\n"
                        elif issue_type in ['person_name_update', 'person_name_update_failed']:
                            person_uuid = issue.get('person_uuid', 'Unknown')
                            given_name = issue.get('given_name', '')
                            family_name = issue.get('family_name', '')
                            person_name = f"{given_name} {family_name}"
                            sk_first_name = issue.get('sk_first_name', '')
                            sk_last_name = issue.get('sk_last_name', '')
                            sk_name = f"{sk_first_name} {sk_last_name}"
                            email_text += f"\n- Person: {person_name}\n"
                            email_text += f"  URL: https://datenkatalog.bs.ch/web/{database_name}/persons/{person_uuid}\n"
                            email_text += f"  Staatskalender name: {sk_name}\n"
                        else:
                            post_label = issue.get('post_label', 'Unknown')
                            post_uuid = issue.get('post_uuid', 'Unknown')
                            email_text += f"\n- {post_label}\n"
                            email_text += f"  URL: https://datenkatalog.bs.ch/web/{database_name}/posts/{post_uuid}\n"
                        
                        # Add specific details based on issue type
                        if issue_type == 'person_without_user':
                            person_name = f"{issue.get('given_name', '')} {issue.get('family_name', '')}"
                            posts_count = issue.get('posts_count', 0)
                            email_text += f"  Posts count: {posts_count}\n"
                            email_text += f"  ACTION REQUIRED: Create a user account for this person.\n"
                            email_text += f"  This person has posts assigned but no associated user account.\n"
                            email_text += f"  To fix this issue:\n"
                            email_text += f"  1. Create a user in Dataspot with this person's email address\n" 
                            email_text += f"  2. Set the user's isPerson field to '{issue.get('family_name', '')}, {issue.get('given_name', '')}'\n"
                            email_text += f"  3. Set the user's access level to EDITOR if this person has any Data Owner posts\n"
                        elif issue_type == 'person_mismatch_missing_email':
                            person_name = f"{issue.get('given_name', '')} {issue.get('family_name', '')}"
                            posts_count = issue.get('posts_count', 0)
                            email_text += f"  Posts count: {posts_count}\n"
                            email_text += f"  ACTION REQUIRED: {person_name} is missing an email in Staatskalender.\n"
                            if posts_count > 0:
                                email_text += f"  The person has {posts_count} post(s) assigned, so you should:\n"
                            else:
                                email_text += f"  The person doesn't have any posts assigned, but it's recommended to:\n"
                            email_text += f"  1. Add an email address for this person in Staatskalender, or\n"
                            email_text += f"  2. Manually create a user in Dataspot and link it to this person\n"
                        elif issue_type == 'person_name_update':
                            person_name = f"{issue.get('given_name', '')} {issue.get('family_name', '')}"
                            sk_first_name = issue.get('sk_first_name', '')
                            sk_last_name = issue.get('sk_last_name', '')
                            sk_name = f"{sk_first_name} {sk_last_name}"
                            email_text += f"  Previous name: {person_name}\n"
                            email_text += f"  Updated name: {sk_name}\n"
                            email_text += f"  The person's name has been automatically updated to match Staatskalender.\n"
                        elif issue_type == 'person_name_update_failed':
                            person_name = f"{issue.get('given_name', '')} {issue.get('family_name', '')}"
                            sk_first_name = issue.get('sk_first_name', '')
                            sk_last_name = issue.get('sk_last_name', '')
                            sk_name = f"{sk_first_name} {sk_last_name}"
                            email_text += f"  Current name: {person_name}\n"
                            email_text += f"  Staatskalender name: {sk_name}\n"
                            email_text += f"  ACTION REQUIRED: The person's name should be updated to match Staatskalender.\n"
                            email_text += f"  Automatic update failed. Please update the person's name manually.\n"
                        elif issue_type == 'access_level_updated':
                            user_email = issue.get('user_email', '')
                            old_level = issue.get('user_access_level_old', 'Unknown')
                            new_level = issue.get('user_access_level_new', ['Unknown'])[0]
                            email_text += f"  User: {user_email}\n"
                            email_text += f"  Previous access level: {old_level}\n"
                            email_text += f"  Updated access level: {new_level}\n"
                            email_text += f"  The user's access level has been automatically updated.\n"
                        elif issue_type == 'access_level_update_failed':
                            user_email = issue.get('user_email', '')
                            current_level = issue.get('user_access_level', 'Unknown')
                            email_text += f"  User: {user_email}\n"
                            email_text += f"  Current access level: {current_level}\n"
                            email_text += f"  ACTION REQUIRED: Failed to update user's access level to EDITOR.\n"
                            email_text += f"  Please update the access level manually.\n"
                        elif issue_type == 'user_person_link_updated':
                            user_email = issue.get('user_email', '')
                            person_name = f"{issue.get('given_name', '')} {issue.get('family_name', '')}"
                            email_text += f"  User: {user_email}\n"
                            email_text += f"  Person: {person_name}\n"
                            email_text += f"  The user has been successfully linked to this person.\n"
                        elif issue_type == 'user_person_link_update_failed':
                            user_email = issue.get('user_email', '')
                            person_name = f"{issue.get('given_name', '')} {issue.get('family_name', '')}"
                            email_text += f"  User: {user_email}\n"
                            email_text += f"  Person: {person_name}\n"
                            email_text += f"  ACTION REQUIRED: Failed to link user to person.\n"
                            email_text += f"  Please link the user to the person manually.\n"
                        elif issue_type == 'person_name_update_failed':
                            person_name = f"{issue.get('given_name', '')} {issue.get('family_name', '')}"
                            sk_first_name = issue.get('sk_first_name', '')
                            sk_last_name = issue.get('sk_last_name', '')
                            sk_name = f"{sk_first_name} {sk_last_name}"
                            email_text += f"  Current name: {person_name}\n"
                            email_text += f"  Staatskalender name: {sk_name}\n"
                            email_text += f"  ACTION REQUIRED: The person's name should be updated to match Staatskalender.\n"
                            email_text += f"  Automatic update failed. Please update the person's name manually.\n"
                        elif issue_type == 'person_mismatch':
                            sk_name = f"{issue.get('sk_first_name', '')} {issue.get('sk_last_name', '')}"
                            ds_name = f"{issue.get('dataspot_first_name', '')} {issue.get('dataspot_last_name', '')}"
                            email_text += f"  Staatskalender name: {sk_name}\n"
                            email_text += f"  Dataspot name: {ds_name}\n"
                        elif issue_type == 'missing_membership':
                            email_text += f"  Issue: No sk_membership_id found\n"
                        elif issue_type in ['invalid_membership', 'missing_person_link']:
                            sk_membership_id = issue.get('sk_membership_id', 'Unknown')
                            email_text += f"  Membership ID: {sk_membership_id}\n"
                        elif issue_type == 'user_created':
                            email_text += f"  User: {issue.get('user_email', 'Unknown')}\n"
                            email_text += f"  The user account has been created.\n"
                        elif issue_type == 'user_creation_failed':
                            email_text += f"  User: {issue.get('user_email', 'Unknown')}\n"
                            email_text += f"  ACTION REQUIRED: Failed to create user account.\n"
                            email_text += f"  Please create the user account manually.\n"
                        
                        email_text += f"  Message: {message}\n"

        # Add error details if any
        if check.get('error'):
            email_text += "\n--- ERROR DETAILS ---\n"
            email_text += check.get('error', 'No details provided')
            email_text += "\n"

    email_text += "\nPlease review the issues and take appropriate actions.\n\n"
    email_text += "Best regards,\n"
    email_text += "Your Dataspot Daily Check Assistant\n\n"
    email_text += "(This is an automatically generated email)\n\n"
    email_text += "PS: Did you spot anything that is incorrectly detected as an issue? Is there any action required by you that feels unncessary and could potentially be automated? Please forward this email to renato.farruggio@bs.ch and tell me what you think could be improved!"

    # Send email with the combined report as attachment
    try:
        report_file = get_combined_report_file_path()
        attachment = report_file if os.path.exists(report_file) else None
        msg = email_helpers.create_email_msg(
            subject=email_subject,
            text=email_text,
            attachment=attachment
        )
        email_helpers.send_email(msg, technical_only=False)
        logging.info("Combined email notification sent successfully")
    except Exception as e:
        logging.error(f"Failed to send combined email notification: {str(e)}")


if __name__ == '__main__':
    logging.basicConfig(
        level=logging.INFO
    )
    logging.info(f"=== CURRENT DATABASE: {config.database_name} ===")
    logging.info(f'Executing {__file__}...')
    main()
    logging.info("")
    logging.info("Execution completed.")
