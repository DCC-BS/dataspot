import logging
import json
import os
import datetime
import config
from src.common import email_helpers
from src.clients.base_client import BaseDataspotClient
from check_daily_posts_occupation import check_posts_occupation
from check_daily_correct_data_owners import check_correct_data_owners


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

    # Initialize client for sql calls
    dataspot_base_client = BaseDataspotClient(base_url=config.base_url, database_name=config.database_name,
                                         scheme_name='NOT_IN_USE', scheme_name_short='NotFound404')

    # Data owner correctness check
    logging.info("")
    logging.info("   Starting Data Owner Correctness Check...")
    logging.info("   " + "-" * 50)
    result = check_correct_data_owners(dataspot_client=dataspot_base_client)
    check_results.append({
        'check_name': 'data_owner_correctness',
        'title': 'Data Owner Correctness Check',
        'description': 'Checks if all Data Owner posts have the correct person assignments.',
        'results': result
    })
    logging.info("")
    logging.info("   Data Owner Correctness Check Completed.")
    logging.info("")

    # Post occupation check
    logging.info("")
    logging.info("   Starting Post Occupation Check...")
    logging.info("   " + "-" * 50)
    result = check_posts_occupation(dataspot_client=dataspot_base_client)
    check_results.append({
        'check_name': 'posts_occupation',
        'title': 'Post Occupation Check',
        'description': 'Checks if all posts are assigned to at least one person.',
        'results': result
    })
    logging.info("")
    logging.info("   Post Occupation Check Completed.")
    logging.info("")

    # Additional checks can be added here
    
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

        # Update summary counters
        if status == 'success':
            combined_report['summary']['successful'] += 1
        elif status == 'warning':
            combined_report['summary']['warnings'] += 1
            # Downgrade overall status if currently successful
            if combined_report['summary']['overall_status'] == 'success':
                combined_report['summary']['overall_status'] = 'warning'
        elif status == 'error':
            combined_report['summary']['errors'] += 1
            # Always downgrade to error if any check has an error
            combined_report['summary']['overall_status'] = 'error'

        # Count total issues
        combined_report['summary']['total_issues'] += len(issues)

        # Add check details to combined report
        combined_report['checks'].append({
            'name': check_name,
            'title': title,
            'description': description,
            'status': status,
            'message': message,
            'issues_count': len(issues),
            'issues': issues,
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
    # Get project root directory (one level up from scripts)
    current_file_path = os.path.abspath(__file__)
    project_root = os.path.dirname(os.path.dirname(current_file_path))

    # Define reports directory in project root
    reports_dir = os.path.join(project_root, "reports")

    # Generate filename with timestamp
    timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
    return os.path.join(reports_dir, f"dataspot_daily_checks_{timestamp}.json")


def get_report_file_path():
    """
    Generate the path for the report file.
    
    Returns:
        str: The path to the report file
    """
    # Get project root directory (one level up from scripts)
    current_file_path = os.path.abspath(__file__)
    project_root = os.path.dirname(os.path.dirname(current_file_path))

    # Define reports directory in project root
    reports_dir = os.path.join(project_root, "reports")

    # Generate filename with timestamp
    timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
    return os.path.join(reports_dir, f"posts_occupation_check_{timestamp}.json")


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
    logging.info(f" Issues: {total_issues} total")

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
            logging.info(f"   Issues: {issues_count}")

        # Group issues by type for easier reading
        issues_by_type = {}
        for issue in check.get('issues', []):
            issue_type = issue.get('type', 'unknown')
            if issue_type not in issues_by_type:
                issues_by_type[issue_type] = []
            issues_by_type[issue_type].append(issue)

        # Log each issue type
        for issue_type, issues in issues_by_type.items():
            logging.info("")
            logging.info(f"   * {issue_type.replace('_', ' ').title()} ({len(issues)})")

            # Log all issues
            for idx, issue in enumerate(issues):
                post_label = issue.get('post_label', 'Unknown')
                post_uuid = issue.get('post_uuid', 'Unknown')
                message = issue.get('message', 'No message')

                logging.info(f"     - {post_label}")
                logging.info(
                    f"       URL: https://datenkatalog.bs.ch/web/{combined_report.get('database_name')}/posts/{post_uuid}")
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
        email_subject = f"[WARNING][{database_name}] Dataspot Daily Checks: {total_issues} Issues Found"
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
    email_text += f"Issues: {total_issues} total\n\n"

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
            email_text += f"Issues: {issues_count}\n"

            # Group issues by type for easier reading
            issues_by_type = {}
            for issue in check.get('issues', []):
                issue_type = issue.get('type', 'unknown')
                if issue_type not in issues_by_type:
                    issues_by_type[issue_type] = []
                issues_by_type[issue_type].append(issue)

            # List the most critical issues first
            priority_order = [
                'name_mismatch', 'no_person_assigned', 'unoccupied_post', 'missing_membership',
                'invalid_membership', 'missing_person_link', 'missing_person_name',
                'missing_dataspot_name', 'dataspot_person_error', 'processing_error'
            ]

            # Sort issue types by priority
            sorted_issue_types = sorted(
                issues_by_type.keys(),
                key=lambda x: priority_order.index(x) if x in priority_order else 999
            )

            # Add all issues by type
            for issue_type in sorted_issue_types:
                issues = issues_by_type[issue_type]
                email_text += f"\n{issue_type.replace('_', ' ').upper()} ISSUES ({len(issues)}):\n"

                # Show all issues
                for idx, issue in enumerate(issues):
                    post_label = issue.get('post_label', 'Unknown')
                    post_uuid = issue.get('post_uuid', 'Unknown')
                    message = issue.get('message', 'No message provided')

                    email_text += f"\n- {post_label}\n"
                    email_text += f"  URL: https://datenkatalog.bs.ch/web/{database_name}/posts/{post_uuid}\n"

                    # Add specific details based on issue type
                    if issue_type == 'name_mismatch':
                        sk_name = f"{issue.get('sk_first_name', '')} {issue.get('sk_last_name', '')}"
                        ds_name = f"{issue.get('dataspot_first_name', '')} {issue.get('dataspot_last_name', '')}"
                        email_text += f"  Staatskalender name: {sk_name}\n"
                        email_text += f"  Dataspot name: {ds_name}\n"
                    elif issue_type == 'missing_membership':
                        email_text += f"  Issue: No membership_id found\n"
                    elif issue_type in ['invalid_membership', 'missing_person_link']:
                        membership_id = issue.get('membership_id', 'Unknown')
                        email_text += f"  Membership ID: {membership_id}\n"

                    email_text += f"  Message: {message}\n"

        # Add error details if any
        if check.get('error'):
            email_text += "\n--- ERROR DETAILS ---\n"
            email_text += check.get('error', 'No details provided')
            email_text += "\n"

    email_text += "\nPlease review the issues and take appropriate actions.\n\n"
    email_text += "Best regards,\n"
    email_text += "Your Dataspot Daily Check Assistant"

    # Send email with the combined report as attachment
    try:
        report_file = get_combined_report_file_path()
        attachment = report_file if os.path.exists(report_file) else None
        msg = email_helpers.create_email_msg(
            subject=email_subject,
            text=email_text,
            attachment=attachment
        )
        email_helpers.send_email(msg)
        logging.info("Combined email notification sent successfully")
    except Exception as e:
        logging.error(f"Failed to send combined email notification: {str(e)}")


def write_data_owner_report(check_results):
    """
    Write data owner check results to a JSON file.
    
    Args:
        check_results (dict): The check results
    """
    report_file = get_data_owner_report_file_path()

    try:
        # Create reports directory if it doesn't exist
        os.makedirs(os.path.dirname(report_file), exist_ok=True)

        # Write report to file
        with open(report_file, 'w', encoding='utf-8') as f:
            json.dump(check_results, f, indent=2, ensure_ascii=False)
        logging.info(f"Detailed data owner report saved to {report_file}")
    except Exception as report_error:
        logging.error(f"Failed to write data owner report file: {str(report_error)}")


def get_data_owner_report_file_path():
    """
    Generate the path for the data owner report file.
    
    Returns:
        str: The path to the report file
    """
    # Get project root directory (one level up from scripts)
    current_file_path = os.path.abspath(__file__)
    project_root = os.path.dirname(os.path.dirname(current_file_path))

    # Define reports directory in project root
    reports_dir = os.path.join(project_root, "reports")

    # Generate filename with timestamp
    timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
    return os.path.join(reports_dir, f"data_owner_check_{timestamp}.json")


def log_detailed_data_owner_report(check_results):
    """
    Log a detailed report of the data owner check results.
    
    Args:
        check_results (dict): The check results dictionary
    """
    logging.info("===== DETAILED DATA OWNER CHECK REPORT =====")
    logging.info(f"Status: {check_results['status']}")
    logging.info(f"Message: {check_results['message']}")

    # Log issues if any
    if check_results['issues']:
        logging.info(f"Issues count: {len(check_results['issues'])}")
        logging.info("--- ISSUES ---")

        # Group issues by type for easier reading
        issues_by_type = {}
        for issue in check_results['issues']:
            issue_type = issue.get('type', 'unknown')
            if issue_type not in issues_by_type:
                issues_by_type[issue_type] = []
            issues_by_type[issue_type].append(issue)

        # Log each issue type
        for issue_type, issues in issues_by_type.items():
            logging.info(f"\n== {issue_type.upper().replace('_', ' ')} ISSUES ({len(issues)}) ==")
            for issue in issues:
                post_label = issue.get('post_label', 'Unknown')
                post_uuid = issue.get('post_uuid', 'Unknown')
                message = issue.get('message', 'No message provided')

                logging.info(f"- {post_label}")
                logging.info(f"  URL: https://datenkatalog.bs.ch/web/{config.database_name}/posts/{post_uuid}")

                # Add specific details based on issue type
                if issue_type == 'name_mismatch':
                    sk_name = f"{issue.get('sk_first_name', '')} {issue.get('sk_last_name', '')}"
                    ds_name = f"{issue.get('dataspot_first_name', '')} {issue.get('dataspot_last_name', '')}"
                    logging.info(f"  Staatskalender name: {sk_name}")
                    logging.info(f"  Dataspot name: {ds_name}")
                elif issue_type == 'missing_membership':
                    logging.info(f"  Issue: No membership_id found")
                elif issue_type in ['invalid_membership', 'missing_person_link']:
                    membership_id = issue.get('membership_id', 'Unknown')
                    logging.info(f"  Membership ID: {membership_id}")

                logging.info(f"  Message: {message}")

    # Log error if any
    if check_results['error']:
        logging.info("--- ERROR DETAILS ---")
        logging.info(check_results['error'])

    logging.info("=============================================")


def create_data_owner_email(check_results, database_name):
    """
    Create email content based on data owner check results.
    
    Args:
        check_results (dict): Check result data
        database_name (str): Name of the database
    
    Returns:
        tuple: (email_subject, email_text, should_send)
    """
    is_error = check_results['status'] == 'error'
    has_issues = len(check_results['issues']) > 0

    # Don't send email if everything is fine
    if not is_error and not has_issues:
        return None, None, False

    # Create email subject
    email_subject = "parser-warning-suppression"
    if is_error:
        email_subject = f"[ERROR][{database_name}] Data Owner Check Failed"
    elif has_issues:
        email_subject = f"[WARNING][{database_name}] Data Owner Check: {len(check_results['issues'])} Issues Found"

    email_text = f"Hi there,\n\n"

    if is_error:
        email_text += f"There was an error during the data owner correctness check.\n"
        email_text += f"Error: {check_results['message']}\n\n"
        if check_results['error']:
            email_text += f"Error details:\n{check_results['error']}\n\n"
    else:
        email_text += f"I've just completed the data owner correctness check for {database_name}.\n\n"

        if has_issues:
            # Group issues by type for better reporting
            issues_by_type = {}
            for issue in check_results['issues']:
                issue_type = issue.get('type', 'unknown')
                if issue_type not in issues_by_type:
                    issues_by_type[issue_type] = []
                issues_by_type[issue_type].append(issue)

            # Include a summary of issues by type
            email_text += "Issues summary:\n"
            for issue_type, issues in issues_by_type.items():
                email_text += f"- {issue_type.replace('_', ' ').title()}: {len(issues)} issues\n"

            email_text += f"\nDetails of issues found:\n\n"

            # List the most critical issues first
            priority_order = [
                'name_mismatch', 'no_person_assigned', 'missing_membership',
                'invalid_membership', 'missing_person_link', 'missing_person_name',
                'missing_dataspot_name', 'dataspot_person_error', 'processing_error'
            ]

            # Sort issue types by priority
            sorted_issue_types = sorted(
                issues_by_type.keys(),
                key=lambda x: priority_order.index(x) if x in priority_order else 999
            )

            # Add details for each issue type
            for issue_type in sorted_issue_types:
                email_text += f"\n{issue_type.replace('_', ' ').upper()} ISSUES ({len(issues_by_type[issue_type])}):\n"

                for issue in issues_by_type[issue_type]:
                    post_label = issue.get('post_label', 'Unknown')
                    post_uuid = issue.get('post_uuid', 'Unknown')
                    message = issue.get('message', 'No message provided')

                    email_text += f"\n- {post_label}\n"
                    email_text += f"  URL: https://datenkatalog.bs.ch/web/{config.database_name}/posts/{post_uuid}\n"

                    # Add specific details based on issue type
                    if issue_type == 'name_mismatch':
                        sk_name = f"{issue.get('sk_first_name', '')} {issue.get('sk_last_name', '')}"
                        ds_name = f"{issue.get('dataspot_first_name', '')} {issue.get('dataspot_last_name', '')}"
                        email_text += f"  Staatskalender name: {sk_name}\n"
                        email_text += f"  Dataspot name: {ds_name}\n"
                    elif issue_type == 'missing_membership':
                        email_text += f"  Issue: No membership_id found\n"
                    elif issue_type in ['invalid_membership', 'missing_person_link']:
                        membership_id = issue.get('membership_id', 'Unknown')
                        email_text += f"  Membership ID: {membership_id}\n"

                    email_text += f"  Message: {message}\n"

            email_text += "\nPlease review these data owner posts and fix the issues.\n\n"

    email_text += "Best regards,\n"
    email_text += "Your Dataspot Data Owner Check Assistant"

    return email_subject, email_text, True


if __name__ == '__main__':
    logging.basicConfig(
        level=logging.INFO,
        format='%(levelname)s:%(name)s:[%(filename)s:%(funcName)s:%(lineno)d] %(message)s'
    )
    logging.info(f"=== CURRENT DATABASE: {config.database_name} ===")
    logging.info(f'Executing {__file__}...')
    main()
    logging.info("")
    logging.info("Execution completed.")
