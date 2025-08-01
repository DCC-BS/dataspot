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
    # Run post occupation check
    check_posts_occupation()
    
    # Run data owner correctness check
    check_correct_data_owners()


def check_posts_occupation():
    """
    Check if all posts are assigned to at least one person.
    
    This method:
    1. Connects to the Dataspot Query API
    2. Executes a SQL query to find posts without any person assigned
    3. Logs the results and generates a report
    """
    logging.info("Starting posts occupation check...")
    
    # Store results for reporting
    check_results = {
        'status': 'pending',
        'message': '',
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
            
            # Store results
            check_results['unoccupied_posts'] = unoccupied_posts
            
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
        # Log final summary
        logging.info(f"Status: {check_results['status']}")
        logging.info(f"Message: {check_results['message']}")
        
        # Write detailed report to file
        write_report(check_results)
        
        # Log detailed report
        log_detailed_check_report(check_results)
        
        # Create and send email notification if needed
        email_subject, email_content, should_send = create_email_content(
            check_results=check_results, 
            database_name=config.database_name
        )
        
        # Send email if there are unoccupied posts or errors
        if should_send:
            try:
                # Create and send email
                report_file = get_report_file_path()
                attachment = report_file if os.path.exists(report_file) else None
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
        # Log final summary
        logging.info(f"Status: {check_results['status']}")
        logging.info(f"Message: {check_results['message']}")
        
        # Write detailed report to file
        write_data_owner_report(check_results)
        
        # Log detailed report
        log_detailed_data_owner_report(check_results)
        
        # Create and send email notification if needed
        email_subject, email_content, should_send = create_data_owner_email(
            check_results=check_results, 
            database_name=config.database_name
        )
        
        # Send email if there are issues or errors
        if should_send:
            try:
                # Create and send email
                report_file = get_data_owner_report_file_path()
                attachment = report_file if os.path.exists(report_file) else None
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


def write_report(check_results):
    """
    Write check results to a JSON file.
    
    Args:
        check_results (dict): The check results
    """
    report_file = get_report_file_path()
    
    try:
        # Create reports directory if it doesn't exist
        os.makedirs(os.path.dirname(report_file), exist_ok=True)
        
        # Write report to file
        with open(report_file, 'w', encoding='utf-8') as f:
            json.dump(check_results, f, indent=2, ensure_ascii=False)
        logging.info(f"Detailed report saved to {report_file}")
    except Exception as report_error:
        logging.error(f"Failed to write report file: {str(report_error)}")


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


def log_detailed_check_report(check_results):
    """
    Log a detailed report of the check results.
    
    Args:
        check_results (dict): The check results dictionary
    """
    logging.info("===== DETAILED POSTS OCCUPATION CHECK REPORT =====")
    logging.info(f"Status: {check_results['status']}")
    logging.info(f"Message: {check_results['message']}")
    
    # Log unoccupied posts if any
    if check_results['unoccupied_posts']:
        logging.info(f"Unoccupied posts count: {len(check_results['unoccupied_posts'])}")
        logging.info("--- UNOCCUPIED POSTS ---")
        for post in check_results['unoccupied_posts']:
            uuid = post.get('uuid', 'Unknown')
            name = post.get('post_label', 'Unknown')
            logging.info(f"- {name} (Link: https://datenkatalog.bs.ch/web/{config.database_name}/posts/{uuid})")
    
    # Log error if any
    if check_results['error']:
        logging.info("--- ERROR DETAILS ---")
        logging.info(check_results['error'])
    
    logging.info("=============================================")


def create_email_content(check_results, database_name):
    """
    Create email content based on check results.
    
    Args:
        check_results (dict): Check result data
        database_name (str): Name of the database
    
    Returns:
        tuple: (email_subject, email_text, should_send)
    """
    is_error = check_results['status'] == 'error'
    has_unoccupied = len(check_results['unoccupied_posts']) > 0
    
    # Don't send email if everything is fine
    if not is_error and not has_unoccupied:
        return None, None, False
    
    # Create email subject
    if is_error:
        email_subject = f"[ERROR][{database_name}] Posts Occupation Check Failed"
    elif has_unoccupied:
        email_subject = f"[WARNING][{database_name}] Posts Occupation Check: {len(check_results['unoccupied_posts'])} Unoccupied Posts"
    
    email_text = f"Hi there,\n\n"
    
    if is_error:
        email_text += f"There was an error during the posts occupation check.\n"
        email_text += f"Error: {check_results['message']}\n\n"
        if check_results['error']:
            email_text += f"Error details:\n{check_results['error']}\n\n"
    else:
        email_text += f"I've just completed the posts occupation check for {database_name}.\n\n"
        
        if has_unoccupied:
            email_text += f"Found {len(check_results['unoccupied_posts'])} posts that are not assigned to any person:\n\n"
            
            # List unoccupied posts
            for post in check_results['unoccupied_posts']:
                uuid = post.get('uuid', 'Unknown')
                name = post.get('post_label', 'Unknown')
                email_text += f"- {name} (Link: https://datenkatalog.bs.ch/web/{config.database_name}/posts/{uuid})\n"
                
            email_text += "\nPlease review these posts and assign them to appropriate persons.\n\n"
    
    email_text += "Best regards,\n"
    email_text += "Your Dataspot Posts Occupation Check Assistant"
    
    return email_subject, email_text, True


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