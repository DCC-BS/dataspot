import logging
import json
import os
import datetime
import traceback
import config
from src.common import email_helpers
from src.common import requests_put
from src.dataspot_auth import DataspotAuth


def main():
    check_posts_occupation()


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
            logging.info(f"- {name} (UUID: {uuid})")
    
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
                email_text += f"- {name} (UUID: {uuid})\n"
                
            email_text += "\nPlease review these posts and assign them to appropriate persons.\n\n"
    
    email_text += "Best regards,\n"
    email_text += "Your Dataspot Posts Occupation Check Assistant"
    
    return email_subject, email_text, True


if __name__ == '__main__':
    logging.basicConfig(
        level=logging.INFO,
        format='%(levelname)s:%(name)s:[%(filename)s:%(funcName)s:%(lineno)d] %(message)s'
    )
    logging.info(f"=== CURRENT DATABASE: {config.database_name} ===")
    logging.info(f'Executing {__file__}...')
    main()