import logging
from typing import Dict, List

import config
from src.common import requests_get, requests_patch
from src.clients.base_client import BaseDataspotClient


def check_5_user_assignment(dataspot_client: BaseDataspotClient, staatskalender_person_email_cache: Dict[str, str] = None) -> Dict[str, any]:
    """
    Check #5: Benutzerkontensynchronisation
    
    This check verifies that all persons with sk_person_id have correct user accounts.
    
    Specifically:
    - For all persons with sk_person_id, it checks:
    - A user with the correct email address from Staatskalender exists
    - The user is correctly linked to the person via the isPerson field
    - If the person has posts, the user has at least EDITOR access rights
    
    Args:
        dataspot_client: Base client for database operations
        staatskalender_person_email_cache: Cache of sk_person_id to email mappings from check_2
        
    Returns:
        dict: Check results including status, issues, and any errors
    """
    logging.debug("Starting Check #5: Benutzerkontensynchronisation...")
    
    result = {
        'status': 'success',
        'message': 'All persons with sk_person_id have correct user assignments.',
        'issues': [],
        'error': None
    }
    
    if not staatskalender_person_email_cache:
        staatskalender_person_email_cache = {}
        logging.warning("No person email cache provided, will not be able to check for missing emails")
    
    try:
        # Get all persons with sk_person_id and their post assignments
        persons_with_sk_id = get_persons_with_sk_person_id_and_posts(dataspot_client)
        
        if not persons_with_sk_id:
            result['message'] = 'No persons with sk_person_id found.'
            return result
            
        logging.info(f"Found {len(persons_with_sk_id)} persons with sk_person_id to verify")
        
        # Get all users from Dataspot
        users = get_all_users(dataspot_client)
        logging.info(f"Found {len(users)} users in the system")
        
        # Create lookup dictionary of users by email
        users_by_email = {}
        for user in users:
            if user['email']:
                users_by_email[user['email'].lower()] = user
                
        # Create lookup dictionary of users by linked person
        users_by_person = {}
        for user in users:
            if user['linked_person_uuid']:
                users_by_person[user['linked_person_uuid']] = user
        
        # Process person-user assignments
        for person in persons_with_sk_id:
            person_uuid = person['person_uuid']
            sk_person_id = person['sk_person_id'].strip('"') if person.get('sk_person_id') else None
            person_name = f"{person['given_name']} {person['family_name']}"
            has_posts = person['posts_count'] > 0
            
            logging.debug(f"Checking person: {person_name} (UUID: {person_uuid}) - Has posts: {has_posts}")
            
            # Get email from cache
            email = staatskalender_person_email_cache.get(sk_person_id)
            
            if not email:
                # Report missing email in Staatskalender
                result['issues'].append({
                    'type': 'person_mismatch_missing_email',
                    'person_uuid': person_uuid,
                    'given_name': person['given_name'],
                    'family_name': person['family_name'],
                    'sk_person_id': sk_person_id,
                    'posts_count': person['posts_count'],
                    'message': f"Person {person_name} has no email address in Staatskalender",
                    'remediation_attempted': False,
                    'remediation_success': False
                })
                logging.info(f"Person {person_name} (UUID: {person_uuid}) has no email address in Staatskalender")
                continue
                
            # Find user with this email
            user = users_by_email.get(email.lower())
            
            if not user:
                # Report missing user account
                result['issues'].append({
                    'type': 'person_without_user',
                    'person_uuid': person_uuid,
                    'given_name': person['given_name'],
                    'family_name': person['family_name'],
                    'sk_person_id': sk_person_id,
                    'posts_count': person['posts_count'],
                    'message': f"Person {person_name} has {person['posts_count']} posts but no associated user account with email {email}",
                    'remediation_attempted': False,
                    'remediation_success': False
                })
                logging.info(f"Person {person_name} (UUID: {person_uuid}) has no associated user account with email {email}")
                continue
                
            # Check if user is linked to correct person
            if user['linked_person_uuid'] != person_uuid:
                # Report incorrect person link
                result['issues'].append({
                    'type': 'incorrect_person_link',
                    'person_uuid': person_uuid,
                    'given_name': person['given_name'],
                    'family_name': person['family_name'],
                    'sk_person_id': sk_person_id,
                    'posts_count': person['posts_count'],
                    'user_uuid': user['user_uuid'],
                    'user_email': user['email'],
                    'user_linked_person_uuid': user['linked_person_uuid'],
                    'message': f"User {user['email']} is not correctly linked to person {person_name}",
                    'remediation_attempted': False,
                    'remediation_success': False
                })
                logging.info(f"User {user['email']} is not correctly linked to person {person_name} (UUID: {person_uuid})")
                
            # Check access level if person has posts
            if has_posts and user['access_level'] not in ['EDITOR', 'ADMIN']:
                # Report insufficient access rights
                result['issues'].append({
                    'type': 'insufficient_access_rights',
                    'person_uuid': person_uuid,
                    'given_name': person['given_name'],
                    'family_name': person['family_name'],
                    'sk_person_id': sk_person_id,
                    'posts_count': person['posts_count'],
                    'user_uuid': user['user_uuid'],
                    'user_email': user['email'],
                    'user_access_level': user['access_level'],
                    'message': f"User {user['email']} has insufficient access rights ({user['access_level']}) but is assigned to {person['posts_count']} posts",
                    'remediation_attempted': False,
                    'remediation_success': False
                })
                logging.info(f"User {user['email']} (linked to {person_name}) has insufficient access rights: {user['access_level']}")
            
        # Update final status and message
        if result['issues']:
            issue_count = len(result['issues'])
            result['status'] = 'warning'
            result['message'] = f"Check #5: Found {issue_count} issues with user assignments"
            logging.info(f"Check finished: Found {issue_count} issues with user assignments")
        else:
            logging.info("Check finished: All persons have correct user assignments")
    
    except Exception as e:
        result['status'] = 'error'
        result['error'] = str(e)
        result['message'] = f"Error in Check #5 (Benutzerkontensynchronisation): {str(e)}"
        logging.error(f"Error in Check #5 (Benutzerkontensynchronisation): {str(e)}", exc_info=True)
    
    return result


def get_persons_with_sk_person_id_and_posts(dataspot_client: BaseDataspotClient) -> List[Dict[str, any]]:
    """
    Get all persons with sk_person_id and count their assigned posts.
        
    Returns:
        List of dicts with person info and post count
    """
    query = """
    SELECT 
        p.id AS person_uuid,
        p.given_name,
        p.family_name,
        cp.value AS sk_person_id,
        COUNT(DISTINCT hp.holds_post) AS posts_count
    FROM 
        person_view p
    JOIN
        customproperties_view cp ON p.id = cp.resource_id AND cp.name = 'sk_person_id'
    LEFT JOIN
        holdspost_view hp ON p.id = hp.resource_id
    GROUP BY
        p.id, p.given_name, p.family_name, cp.value
    ORDER BY
        p.family_name, p.given_name
    """
    return dataspot_client.execute_query_api(sql_query=query)


def get_all_users(dataspot_client: BaseDataspotClient) -> List[Dict[str, any]]:
    """
    Get all non-service users from Dataspot.
        
    Returns:
        List of dicts with user info
    """
    query = """
    SELECT
        u.id AS user_uuid,
        u.login_id AS email,
        u.access_level,
        u.is_person AS linked_person_uuid,
        u.name AS display_name
    FROM
        user_view u
    WHERE 
        u.service_user IS NULL OR u.service_user = false
    ORDER BY
        u.login_id
    """
    return dataspot_client.execute_query_api(sql_query=query)
