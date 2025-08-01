import logging
import traceback
from typing import Any, Dict

import config
from src.clients.base_client import BaseDataspotClient

def check_posts_occupation(dataspot_client: BaseDataspotClient) -> Dict[str, Any]:
    """
    Check if all posts are assigned to at least one person.

    This method:
    1. Executes a SQL query to find posts without any person assigned

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
        result = dataspot_client.execute_query_api(sql_query=sql_query)

        # Process results - the result is a direct list, not wrapped in a 'data' field
        if isinstance(result, list):
            unoccupied_posts = result
            unoccupied_count = len(unoccupied_posts)

            logging.info(f"Found {unoccupied_count} unoccupied posts")
            for unoccupied_post in unoccupied_posts:
                logging.info(f" - {unoccupied_post['post_label']} ({dataspot_client.base_url}/web/{config.database_name}/posts/{unoccupied_post['uuid']})")

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
        logging.info("")
        logging.info(f"Posts occupation check - Status: {check_results['status']}")
        logging.info(f"Posts occupation check - Message: {check_results['message']}")

        if check_results['issues']:
            logging.info(f"Posts occupation check - Found {len(check_results['issues'])} issues")

        return check_results