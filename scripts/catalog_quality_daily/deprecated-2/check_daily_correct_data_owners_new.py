import logging
import traceback
from typing import Any, Dict, List, Optional

import config
from src.clients.base_client import BaseDataspotClient
from src.common import requests_get
import requests

# Import new comprehensive sync module
from scripts.catalog_quality_daily.post_person_sync import sync_posts_with_staatskalender_persons


def get_data_owner_posts(dataspot_client: BaseDataspotClient) -> List[Dict[str, Any]]:
    """
    Execute query to find all Data Owner posts.
    
    Args:
        dataspot_client: The client for connecting to Dataspot API
        
    Returns:
        List of Data Owner posts
    """
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
    result = dataspot_client.execute_query_api(sql_query=sql_query)

    return result


def check_correct_data_owners(dataspot_client: BaseDataspotClient) -> Dict[str, Any]:
    """
    Check if all Data Owner posts are assigned to the correct person according to Staatskalender.
    
    For each Data Owner post, this method:
    1. Retrieves Staatskalender person ID(s) from membership data (including secondary membership_id)
    2. Ensures the person exists with the correct name from Staatskalender
    3. Updates person records with correct sk_person_id if needed
    4. Updates person-to-post link if needed
       
    Note: User account verification has been moved to a separate check (check_daily_user_accounts.py)
    """
    # Store results for reporting
    check_results = {
        'status': 'pending',
        'message': '',
        'issues': [],
        'error': None
    }

    try:
        # Get all Data Owner posts
        data_owner_posts = get_data_owner_posts(dataspot_client)

        # Process results
        if data_owner_posts:
            post_count = len(data_owner_posts)
            logging.info(f"Found {post_count} Data Owner posts to check")
            
            # Process Data Owner posts with comprehensive approach
            logging.info("")
            logging.info("===== SYNCING STAATSKALENDER DATA AND VERIFYING POST ASSIGNMENTS =====")
            
            # Uses comprehensive approach that handles:
            # - Both primary and secondary membership IDs
            # - Person creation and updates
            # - Post-to-person linking
            
            all_issues = sync_posts_with_staatskalender_persons(dataspot_client, data_owner_posts)
            
            logging.info(f"Data Owner verification completed with {len(all_issues)} total issues")
            check_results['issues'] = all_issues
            
            # Update check results based on issues found
            if len(all_issues) == 0:
                check_results['status'] = 'success'
                check_results['message'] = "All Data Owner posts have correct person assignments."
            else:
                check_results['status'] = 'warning'
                check_results['message'] = f"Found {len(all_issues)} issues with Data Owner posts."
        else:
            check_results['status'] = 'error'
            check_results['message'] = "Failed to retrieve Data Owner posts from the API."
            check_results['error'] = "Invalid response format from the Query API"

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
        logging.info("")
        logging.info(f"Data Owner check - Status: {check_results['status']}")
        logging.info(f"Data Owner check - Message: {check_results['message']}")

        if check_results['issues']:
            logging.info(f"Data Owner check - Found {len(check_results['issues'])} issues")

        return check_results
