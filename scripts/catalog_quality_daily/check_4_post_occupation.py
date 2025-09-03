import logging
from typing import Dict, List

import config
from src.clients.base_client import BaseDataspotClient


def check_4_post_occupation(dataspot_client: BaseDataspotClient) -> Dict[str, any]:
    """
    Check #4: Postenbesetzungsprüfung
    
    This check verifies that all posts are occupied by at least one person.
    
    Specifically:
    - For all posts, it checks:
        - At least one person is assigned to the post
    
    If not:
    - If a post has no assigned person, this is reported
    - All unoccupied posts are documented in the report
    
    Args:
        dataspot_client: Base client for database operations
        
    Returns:
        dict: Check results including status, issues, and any errors
    """
    logging.debug("Starting Check #4: Post Occupation check...")
    
    result = {
        'status': 'success',
        'message': 'All posts are occupied by at least one person.',
        'issues': [],
        'error': None
    }
    
    try:
        # Get all posts without any assigned person
        unoccupied_posts = get_unoccupied_posts(dataspot_client)
        
        if not unoccupied_posts:
            result['message'] = 'All posts are occupied by at least one person.'
            logging.info(f"Check finished: All posts are occupied")
            return result
        
        # Process each unoccupied post
        for post in unoccupied_posts:
            post_uuid = post.get('post_uuid')
            post_label = post.get('post_label')
            
            result['issues'].append({
                'type': 'unoccupied_post',
                'post_uuid': post_uuid,
                'post_label': post_label,
                'message': f"Post {post_label} has no person assigned",
                'remediation_attempted': False,
                'remediation_success': False
            })
        
        # Update status based on issues
        if result['issues']:
            result['status'] = 'warning'
            result['message'] = f"Check #4: Found {len(result['issues'])} post(s) without any person assigned"
            logging.info(f"Check finished: Found {len(result['issues'])} post(s) without any person assigned")
        else:
            logging.info(f"Check finished: All posts are correctly occupied")
        

    
    except Exception as e:
        result['status'] = 'error'
        result['error'] = str(e)
        result['message'] = f"Error in Check #4 (Post Occupation): {str(e)}"
        logging.error(f"Error in Check #4 (Post Occupation): {str(e)}", exc_info=True)
    
    return result


def get_unoccupied_posts(dataspot_client: BaseDataspotClient) -> List[Dict[str, any]]:
    """
    Get all posts without any person assigned.
    
    Args:
        dataspot_client: Database client
        
    Returns:
        list: Posts without any assigned person
    """
    query = """
    SELECT 
        p.id AS post_uuid,
        p.label AS post_label
    FROM 
        post_view p
    WHERE 
        NOT EXISTS (
            SELECT 1 
            FROM holdspost_view h 
            WHERE h.holds_post = p.id
        )
    ORDER BY 
        p.label
    """
    
    return dataspot_client.execute_query_api(sql_query=query)
