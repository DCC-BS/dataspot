import logging
import json
import os
import datetime
import traceback
import config
from src.common import email_helpers
from src.common import requests_put, requests_get
from src.dataspot_auth import DataspotAuth


def main():
    """
    Main function to run all daily checks and generate a combined report.
    """
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
    
    # Post occupation check
    logging.info("Starting post occupation check...")
    result = check_posts_occupation()
    check_results.append({
        'check_name': 'posts_occupation',
        'title': 'Post Occupation Check',
        'description': 'Checks if all posts are assigned to at least one person.',
        'results': result
    })
    
    # Data owner correctness check
    logging.info("Starting data owner correctness check...")
    result = check_correct_data_owners()
    check_results.append({
        'check_name': 'data_owner_correctness',
        'title': 'Data Owner Correctness Check',
        'description': 'Checks if all Data Owner posts have the correct person assignments.',
        'results': result
    })
    
    # Additional checks can be added here
    
    return check_results


def check_posts_occupation():
    """
    Check if all posts are assigned to at least one person.
    
    This method:
    1. Connects to the Dataspot Query API
    2. Executes a SQL query to find posts without any person assigned
    
    Returns:
        dict: Check results in standardized format
    """
    # Store results for reporting
    check_results = {
        'status': 'pending',
        'message': '',
        'issues': [],
        'unoccupied_posts': [],
        'error': None
    }
    
    try:
        # SQL query to find posts that are not occupied by any person
        sql_query = """
        SELECT 
            id AS UUID, 
            label AS post_label
        FROM 
            post_view p
        WHERE 
            NOT EXISTS (
                SELECT 1 
                FROM holdspost_view h 
                WHERE h.holds_post = p.id
            )
            AND p.status = 'WORKING';
        """
        
        # Execute query via Dataspot Query API
        logging.info("Executing query to find unoccupied posts...")
        result = execute_query_api(sql_query=sql_query)
        
        # Process results - the result is a direct list, not wrapped in a 'data' field
        if isinstance(result, list):
            unoccupied_posts = result
            unoccupied_count = len(unoccupied_posts)
            
            logging.info(f"Found {unoccupied_count} unoccupied posts")
            
            # Store original unoccupied posts for backward compatibility
            check_results['unoccupied_posts'] = unoccupied_posts
            
            # Convert unoccupied posts to standard issues format for consistency
            for post in unoccupied_posts:
                issue = {
                    'type': 'unoccupied_post',
                    'post_uuid': post.get('uuid'),
                    'post_label': post.get('post_label'),
                    'message': f"Post is not assigned to any person"
                }
                check_results['issues'].append(issue)
            
            if unoccupied_count == 0:
                check_results['status'] = 'success'
                check_results['message'] = "All posts are occupied by at least one person."
            else:
                check_results['status'] = 'warning'
                check_results['message'] = f"Found {unoccupied_count} posts that are not occupied by any person."
        else:
            check_results['status'] = 'error'
            check_results['message'] = "Failed to retrieve unoccupied posts data from the API."
            check_results['error'] = f"Invalid response format from the Query API: {type(result)}"
            
    except Exception as e:
        # Capture error information
        error_message = str(e)
        error_traceback = traceback.format_exc()
        logging.error(f"Exception occurred during check: {error_message}")
        logging.error(f"Traceback: {error_traceback}")
        
        # Update the check_results with error status
        check_results['status'] = 'error'
        check_results['message'] = f"Posts occupation check failed. Error: {error_message}."
        check_results['error'] = error_traceback
    
    finally:
        # Log a brief summary
        logging.info(f"Posts occupation check - Status: {check_results['status']}")
        logging.info(f"Posts occupation check - Message: {check_results['message']}")
        
        if check_results['issues']:
            logging.info(f"Posts occupation check - Found {len(check_results['issues'])} issues")
        
        logging.info("Posts occupation check process finished")
        logging.info("===============================================")
        
        return check_results

def check_correct_data_owners():
    """
    Check if all Data Owner posts are assigned to the correct person according to Staatskalender.
    
    This method:
    1. Executes a SQL query to find all Data Owner posts
    2. For each post:
       - Checks if it has a membership_id
       - Verifies the membership exists in Staatskalender
       - Compares the person in Dataspot with the person in Staatskalender
    3. Logs the results and generates a report
    """
    logging.info("Starting Data Owner correctness check...")
    
    # Store results for reporting
    check_results = {
        'status': 'pending',
        'message': '',
        'issues': [],
        'error': None
    }
    
    try:
        # SQL query to find all Data Owner posts
        sql_query = """
        SELECT 
          p.id AS uuid,
          p.label AS post_label
        FROM 
          post_view p
        WHERE 
          p.has_role = (
            SELECT 
              r.id 
            FROM 
              role_view r 
            WHERE 
              r.label = 'Data Owner'
          );
        """
        
        # Execute query via Dataspot Query API
        logging.info("Executing query to find all Data Owner posts...")
        result = execute_query_api(sql_query=sql_query)
        
        # Process results
        if isinstance(result, list):
            data_owner_posts = result
            post_count = len(data_owner_posts)
            
            logging.info(f"Found {post_count} Data Owner posts to check")
            
            # Track issues
            issues_count = 0
            
            # Examine each post
            for idx, post in enumerate(data_owner_posts):
                post_uuid = post.get('uuid')
                post_label = post.get('post_label', 'Unknown')
                
                logging.info(f"[{idx+1}/{post_count}] Checking Data Owner post: {post_label}")
                
                # Step 1: Get post details from Dataspot
                post_url = f"{config.base_url}/api/{config.database_name}/posts/{post_uuid}"
                
                auth = DataspotAuth()
                post_response = requests_get(
                    url=post_url,
                    headers=auth.get_headers()
                )
                
                # Check if we got valid post data
                if post_response.status_code != 200:
                    issue = {
                        'type': 'error',
                        'post_uuid': post_uuid,
                        'post_label': post_label,
                        'message': f"Could not retrieve post data from Dataspot. Status code: {post_response.status_code}"
                    }
                    check_results['issues'].append(issue)
                    issues_count += 1
                    continue
                
                post_data = post_response.json().get('asset', {})
                
                # Step 2: Check if post has membership_id
                membership_id = post_data.get('customProperties', {}).get('membership_id')
                if not membership_id:
                    # Log the issue immediately
                    logging.warning(f"MISSING MEMBERSHIP: Post '{post_label}' (UUID: {post_uuid}) does not have a membership_id")
                    
                    issue = {
                        'type': 'missing_membership',
                        'post_uuid': post_uuid,
                        'post_label': post_label,
                        'message': f"Post does not have a membership_id"
                    }
                    check_results['issues'].append(issue)
                    issues_count += 1
                    continue
                
                # Step 3: Check if membership exists in Staatskalender
                logging.info(f"Checking membership_id {membership_id} in Staatskalender...")
                membership_url = f"https://staatskalender.bs.ch/api/memberships/{membership_id}"
                membership_response = requests_get(url=membership_url)
                
                if membership_response.status_code != 200:
                    # Log the invalid membership ID immediately
                    logging.warning(f"INVALID MEMBERSHIP: Post '{post_label}' (UUID: {post_uuid}) has invalid membership_id '{membership_id}'. Status code: {membership_response.status_code}")
                    
                    issue = {
                        'type': 'invalid_membership',
                        'post_uuid': post_uuid,
                        'post_label': post_label,
                        'membership_id': membership_id,
                        'message': f"Membership ID not found in Staatskalender. Status code: {membership_response.status_code}"
                    }
                    check_results['issues'].append(issue)
                    issues_count += 1
                    continue
                
                membership_data = membership_response.json()
                
                # Extract person link from membership data
                try:
                    person_link = None
                    for item in membership_data.get('collection', {}).get('items', []):
                        for link in item.get('links', []):
                            if link.get('rel') == 'person':
                                person_link = link.get('href')
                                break
                        if person_link:
                            break
                    
                    if not person_link:
                        issue = {
                            'type': 'missing_person_link',
                            'post_uuid': post_uuid,
                            'post_label': post_label,
                            'membership_id': membership_id,
                            'message': f"Could not find person link in membership data"
                        }
                        check_results['issues'].append(issue)
                        issues_count += 1
                        continue
                    
                    # Step 4: Get person data from Staatskalender
                    logging.info(f"Retrieving person data from Staatskalender: {person_link}")
                    person_response = requests_get(url=person_link)
                    
                    if person_response.status_code != 200:
                        issue = {
                            'type': 'person_data_error',
                            'post_uuid': post_uuid,
                            'post_label': post_label,
                            'membership_id': membership_id,
                            'message': f"Could not retrieve person data from Staatskalender. Status code: {person_response.status_code}"
                        }
                        check_results['issues'].append(issue)
                        issues_count += 1
                        continue
                    
                    # Extract first and last name from Staatskalender person
                    person_data = person_response.json()
                    sk_first_name = None
                    sk_last_name = None
                    
                    for item in person_data.get('collection', {}).get('items', []):
                        for data_item in item.get('data', []):
                            if data_item.get('name') == 'first_name':
                                sk_first_name = data_item.get('value')
                            elif data_item.get('name') == 'last_name':
                                sk_last_name = data_item.get('value')
                    
                    if not sk_first_name or not sk_last_name:
                        issue = {
                            'type': 'missing_person_name',
                            'post_uuid': post_uuid,
                            'post_label': post_label,
                            'membership_id': membership_id,
                            'message': f"Could not extract name from Staatskalender person data"
                        }
                        check_results['issues'].append(issue)
                        issues_count += 1
                        continue
                    
                    # Step 5: Find associated person in Dataspot
                    # Get a list of persons holding this post
                    person_query_url = f"{config.base_url}/api/{config.database_name}/posts/{post_uuid}/agentOf"
                    person_query_response = requests_get(
                        url=person_query_url,
                        headers=auth.get_headers()
                    )
                    
                    if person_query_response.status_code != 200:
                        issue = {
                            'type': 'dataspot_person_error',
                            'post_uuid': post_uuid,
                            'post_label': post_label,
                            'membership_id': membership_id,
                            'message': f"Could not retrieve associated person from Dataspot. Status code: {person_query_response.status_code}"
                        }
                        check_results['issues'].append(issue)
                        issues_count += 1
                        continue
                    
                    # Check if any person holds this post
                    person_data = person_query_response.json()
                    if not person_data or '_embedded' not in person_data or 'persons' not in person_data.get('_embedded', {}):
                        issue = {
                            'type': 'no_person_assigned',
                            'post_uuid': post_uuid,
                            'post_label': post_label,
                            'membership_id': membership_id,
                            'sk_first_name': sk_first_name,
                            'sk_last_name': sk_last_name,
                            'message': f"No person assigned to this post in Dataspot"
                        }
                        check_results['issues'].append(issue)
                        issues_count += 1
                        continue
                    
                    # Get the first person (should be only one)
                    dataspot_persons = person_data.get('_embedded', {}).get('persons', [])
                    if not dataspot_persons:
                        issue = {
                            'type': 'no_person_assigned',
                            'post_uuid': post_uuid,
                            'post_label': post_label,
                            'membership_id': membership_id,
                            'sk_first_name': sk_first_name,
                            'sk_last_name': sk_last_name,
                            'message': f"No person assigned to this post in Dataspot"
                        }
                        check_results['issues'].append(issue)
                        issues_count += 1
                        continue
                    
                    # Get the person's details
                    dataspot_person = dataspot_persons[0]
                    dataspot_person_uuid = dataspot_person.get('id')
                    
                    # Get detailed person information
                    person_detail_url = f"{config.base_url}/api/{config.database_name}/persons/{dataspot_person_uuid}"
                    person_detail_response = requests_get(
                        url=person_detail_url,
                        headers=auth.get_headers()
                    )
                    
                    if person_detail_response.status_code != 200:
                        issue = {
                            'type': 'dataspot_person_details_error',
                            'post_uuid': post_uuid,
                            'post_label': post_label,
                            'membership_id': membership_id,
                            'dataspot_person_uuid': dataspot_person_uuid,
                            'message': f"Could not retrieve person details from Dataspot. Status code: {person_detail_response.status_code}"
                        }
                        check_results['issues'].append(issue)
                        issues_count += 1
                        continue
                    
                    # Extract person details
                    person_details = person_detail_response.json().get('asset', {})
                    dataspot_first_name = person_details.get('givenName')
                    dataspot_last_name = person_details.get('familyName')
                    
                    # Compare names
                    if not dataspot_first_name or not dataspot_last_name:
                        issue = {
                            'type': 'missing_dataspot_name',
                            'post_uuid': post_uuid,
                            'post_label': post_label,
                            'membership_id': membership_id,
                            'dataspot_person_uuid': dataspot_person_uuid,
                            'sk_first_name': sk_first_name,
                            'sk_last_name': sk_last_name,
                            'message': f"Person in Dataspot has missing name information"
                        }
                        check_results['issues'].append(issue)
                        issues_count += 1
                        continue
                    
                    # Check if names match
                    if sk_first_name != dataspot_first_name or sk_last_name != dataspot_last_name:
                        issue = {
                            'type': 'name_mismatch',
                            'post_uuid': post_uuid,
                            'post_label': post_label,
                            'membership_id': membership_id,
                            'dataspot_person_uuid': dataspot_person_uuid,
                            'sk_first_name': sk_first_name,
                            'sk_last_name': sk_last_name,
                            'dataspot_first_name': dataspot_first_name,
                            'dataspot_last_name': dataspot_last_name,
                            'message': f"Person name mismatch: Staatskalender ({sk_first_name} {sk_last_name}) vs. Dataspot ({dataspot_first_name} {dataspot_last_name})"
                        }
                        check_results['issues'].append(issue)
                        issues_count += 1
                    else:
                        logging.info(f"✓ Data Owner post '{post_label}' has correct person assignment: {dataspot_first_name} {dataspot_last_name}")
                
                except Exception as e:
                    # Capture any other errors that might occur during processing
                    issue = {
                        'type': 'processing_error',
                        'post_uuid': post_uuid,
                        'post_label': post_label,
                        'membership_id': membership_id if 'membership_id' in locals() else None,
                        'message': f"Error processing post: {str(e)}"
                    }
                    check_results['issues'].append(issue)
                    issues_count += 1
                    logging.error(f"Error processing post {post_label}: {str(e)}")
            
            # Update check results based on issues found
            if issues_count == 0:
                check_results['status'] = 'success'
                check_results['message'] = "All Data Owner posts have correct person assignments."
            else:
                check_results['status'] = 'warning'
                check_results['message'] = f"Found {issues_count} issues with Data Owner posts."
            
        else:
            check_results['status'] = 'error'
            check_results['message'] = "Failed to retrieve Data Owner posts from the API."
            check_results['error'] = f"Invalid response format from the Query API: {type(result)}"
            
    except Exception as e:
        # Capture error information
        error_message = str(e)
        error_traceback = traceback.format_exc()
        logging.error(f"Exception occurred during check: {error_message}")
        logging.error(f"Traceback: {error_traceback}")
        
        # Update the check_results with error status
        check_results['status'] = 'error'
        check_results['message'] = f"Data Owner correctness check failed. Error: {error_message}."
        check_results['error'] = error_traceback
    
    finally:
        # Log a brief summary
        logging.info(f"Data Owner check - Status: {check_results['status']}")
        logging.info(f"Data Owner check - Message: {check_results['message']}")
        
        if check_results['issues']:
            logging.info(f"Data Owner check - Found {len(check_results['issues'])} issues")
        
        logging.info("Data Owner correctness check process finished")
        logging.info("===============================================")
        
        return check_results

def execute_query_api(sql_query):
    """
    Execute a query using the Dataspot Query API and return JSON results.

    Args:
        sql_query (str): The SQL query to execute

    Returns:
        dict: The query results
    """
    logging.info("Connecting to Dataspot Query API...")

    # Prepare request data
    query_data = {
        "sql": sql_query
    }

    # API endpoint
    base_url = config.base_url
    database = config.database_name
    endpoint = f"{base_url}/api/{database}/queries/download?format=JSON"

    # Create DataspotAuth for authentication
    auth = DataspotAuth()
    
    logging.info(f"Sending query to endpoint: {endpoint}")

    response = requests_put(
        url=endpoint,
        json=query_data,
        headers=auth.get_headers()
    )

    return response.json()


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
    logging.info("===== DATASPOT DAILY CHECKS SUMMARY REPORT =====")
    
    # Log summary
    summary = combined_report.get('summary', {})
    overall_status = summary.get('overall_status', 'unknown').upper()
    total_checks = summary.get('total_checks', 0)
    successful = summary.get('successful', 0)
    warnings = summary.get('warnings', 0)
    errors = summary.get('errors', 0)
    total_issues = summary.get('total_issues', 0)
    
    logging.info(f"Overall Status: {overall_status}")
    logging.info(f"Database: {combined_report.get('database_name')}")
    logging.info(f"Time: {combined_report.get('timestamp')}")
    logging.info(f"Checks: {total_checks} total - {successful} successful, {warnings} with warnings, {errors} with errors")
    logging.info(f"Issues: {total_issues} total")
    
    # Log details for each check
    for check in combined_report.get('checks', []):
        check_name = check.get('name', 'Unknown')
        title = check.get('title', 'Unknown Check')
        status = check.get('status', 'unknown').upper()
        issues_count = check.get('issues_count', 0)
        
        logging.info(f"\n--- {title.upper()} ({status}) ---")
        logging.info(f"Message: {check.get('message', 'No message')}")
        
        # Log issues for checks with issues
        if issues_count > 0:
            logging.info(f"Issues: {issues_count}")
            
            # Group issues by type for easier reading
            issues_by_type = {}
            for issue in check.get('issues', []):
                issue_type = issue.get('type', 'unknown')
                if issue_type not in issues_by_type:
                    issues_by_type[issue_type] = []
                issues_by_type[issue_type].append(issue)
            
            # Log each issue type
            for issue_type, issues in issues_by_type.items():
                logging.info(f"\n== {issue_type.upper().replace('_', ' ')} ({len(issues)}) ==")
                
                # Log all issues
                for idx, issue in enumerate(issues):
                        post_label = issue.get('post_label', 'Unknown')
                        post_uuid = issue.get('post_uuid', 'Unknown')
                        message = issue.get('message', 'No message')
                        
                        logging.info(f"- {post_label}")
                        logging.info(f"  URL: https://datenkatalog.bs.ch/web/{combined_report.get('database_name')}/posts/{post_uuid}")
                        logging.info(f"  Message: {message}")
        
        # Log error if any
        if check.get('error'):
            logging.info("--- ERROR DETAILS ---")
            logging.info(check.get('error'))
    
    logging.info("\nSee detailed report for more information.")
    logging.info("=============================================")


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