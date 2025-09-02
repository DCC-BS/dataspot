import logging
import traceback
from typing import Any, Dict, List, Optional

import config
from src.clients.base_client import BaseDataspotClient
from src.common import requests_get, requests_patch


def get_all_persons_with_sk_id(dataspot_client: BaseDataspotClient) -> List[Dict[str, Any]]:
    """
    Get all persons in Dataspot that have sk_person_id set.
    
    Args:
        dataspot_client: The Dataspot client
        
    Returns:
        List of persons with sk_person_id
    """
    # SQL query to find all persons with sk_person_id set
    sql_query = """
    SELECT
      p.id,
      p.given_name AS "givenName",
      p.family_name AS "familyName",
      cp.value AS "sk_person_id"
    FROM
      person_view p
    JOIN
      customproperties_view cp ON p.id = cp.resource_id
    WHERE
      cp.name = 'sk_person_id'
      AND cp.value IS NOT NULL
      AND cp.value != ''
    """
    
    # Execute query via Dataspot Query API
    logging.info("Executing query to find all persons with sk_person_id...")
    result = dataspot_client.execute_query_api(sql_query=sql_query)
    
    # Get post assignments in a separate query
    posts_query = """
    SELECT
      p.id AS person_id,
      hp.holds_post AS post_uuid,
      post.label AS post_label,
      post.has_role AS role_id
    FROM
      person_view p
    JOIN
      holdspost_view hp ON p.id = hp.resource_id
    JOIN
      post_view post ON hp.holds_post = post.id
    """
    
    logging.info("Executing query to get post assignments...")
    posts_result = dataspot_client.execute_query_api(sql_query=posts_query)
    
    # Get Data Owner role ID
    do_role_query = """
    SELECT 
      r.id
    FROM 
      role_view r 
    WHERE 
      r.label = 'Data Owner'
    """
    do_role_result = dataspot_client.execute_query_api(sql_query=do_role_query)
    data_owner_role_id = do_role_result[0]['id'] if do_role_result else None
    
    # Build a mapping of person_id to posts
    person_posts = {}
    for row in posts_result:
        person_id = row.get('person_id')
        post_uuid = row.get('post_uuid')
        post_label = row.get('post_label')
        role_id = row.get('role_id')
        
        if person_id not in person_posts:
            person_posts[person_id] = []
        
        if post_uuid:
            is_data_owner = (role_id == data_owner_role_id)
            person_posts[person_id].append({
                'uuid': post_uuid,
                'label': post_label,
                'is_data_owner': is_data_owner
            })
    
    # Process results to match the expected structure
    persons_with_sk_id = []
    for row in result:
        person_id = row.get('id')
        # Get this person's posts from our mapping
        posts = person_posts.get(person_id, [])
        
        # Check if any posts are Data Owner
        has_data_owner_role = any(post.get('is_data_owner', False) for post in posts)
        
        # Format each person to match the structure expected by the rest of the code
        persons_with_sk_id.append({
            'id': person_id,
            'givenName': row.get('givenName'),
            'familyName': row.get('familyName'),
            'sk_person_id': row.get('sk_person_id').strip('"') if row.get('sk_person_id') else None,
            'posts': posts,
            'has_data_owner_role': has_data_owner_role
        })
    
    logging.info(f"Found {len(persons_with_sk_id)} persons with sk_person_id")
    return persons_with_sk_id


def get_sk_person_details(sk_person_id: str) -> Dict[str, Any]:
    """
    Get person details from Staatskalender using sk_person_id.
    
    Args:
        sk_person_id: Staatskalender person ID
        
    Returns:
        Dict with person details or empty dict if failed
    """
    person_url = f"https://staatskalender.bs.ch/person/{sk_person_id}"
    api_url = f"https://staatskalender.bs.ch/api/people/{sk_person_id}"
    
    person_response = requests_get(
        url=api_url,
        headers={}  # No dataspot headers needed
    )
    
    if person_response.status_code != 200:
        logging.error(f"Could not fetch person data from Staatskalender for ID {sk_person_id}. Status code: {person_response.status_code}")
        return {}
    
    person_data = person_response.json()
    first_name = None
    last_name = None
    email = None
    
    for item in person_data.get('collection', {}).get('items', []):
        for data_item in item.get('data', []):
            if data_item.get('name') == 'first_name':
                first_name = data_item.get('value')
            elif data_item.get('name') == 'last_name':
                last_name = data_item.get('value')
            elif data_item.get('name') == 'email':
                email = data_item.get('value').lower() if data_item.get('value') else None
    
    return {
        'first_name': first_name,
        'last_name': last_name,
        'email': email,
        'sk_person_id': sk_person_id,
        'url': person_url
    }


def get_dataspot_users(dataspot_client: BaseDataspotClient) -> List[Dict[str, Any]]:
    """
    Get all users from Dataspot.
    
    Args:
        dataspot_client: The Dataspot client
        
    Returns:
        List of users
    """
    users_url = f"{dataspot_client.base_url}/api/{dataspot_client.database_name}/tenants/{config.tenant_name}/download?format=JSON"
    users_response = requests_get(
        url=users_url,
        headers=dataspot_client.auth.get_headers()
    )
    
    if users_response.status_code != 200:
        logging.error(f"Could not fetch users from Dataspot. Status code: {users_response.status_code}")
        return []
    
    users = users_response.json()
    logging.info(f"Found {len(users)} users in Dataspot")
    return users


def check_user_accounts(dataspot_client: BaseDataspotClient) -> Dict[str, Any]:
    """
    Check if all persons with sk_person_id have corresponding user accounts.
    
    This check:
    1. Finds all persons with sk_person_id
    2. Verifies email from Staatskalender matches
    3. Ensures a corresponding user exists with correct access level
    
    Args:
        dataspot_client: The Dataspot client
        
    Returns:
        Dict with check results
    """
    # Store results for reporting
    check_results = {
        'status': 'pending',
        'message': '',
        'issues': [],
        'error': None
    }
    
    try:
        # Get all persons with sk_person_id
        persons_with_sk_id = get_all_persons_with_sk_id(dataspot_client)
        
        # Get all users from Dataspot
        users = get_dataspot_users(dataspot_client)
        
        # Build a mapping of email to user for faster lookups
        users_by_email = {}
        for user in users:
            email = user.get('email', '').lower()
            if email:
                users_by_email[email] = user
        
        # Build a mapping of isPerson to user
        users_by_person_name = {}
        for user in users:
            is_person = user.get('isPerson')
            if is_person:
                users_by_person_name[is_person] = user
        
        # Check each person has a user account
        for person in persons_with_sk_id:
            person_id = person.get('id')
            given_name = person.get('givenName')
            family_name = person.get('familyName')
            sk_person_id = person.get('sk_person_id')
            has_data_owner_role = person.get('has_data_owner_role', False)
            posts = person.get('posts', [])
            
            # Get person details from Staatskalender
            sk_person = get_sk_person_details(sk_person_id)
            sk_email = sk_person.get('email')
            
            # Format person name as it would appear in isPerson
            person_name_format = f"{family_name}, {given_name}"
            
            # Issue: Person missing email in Staatskalender
            if not sk_email:
                check_results['issues'].append({
                    'type': 'person_missing_email',
                    'person_uuid': person_id,
                    'given_name': given_name,
                    'family_name': family_name,
                    'sk_person_id': sk_person_id,
                    'posts_count': len(posts),
                    'message': f"Person {given_name} {family_name} missing email in Staatskalender",
                    'remediation_attempted': False,
                    'remediation_reason': "Cannot create user without email address"
                })
                continue
            
            # Check if user exists by email
            user = users_by_email.get(sk_email)
            
            # If no user by email, check by person name
            if not user:
                user = users_by_person_name.get(person_name_format)
            
            # Issue: No user found for this person
            if not user:
                check_results['issues'].append({
                    'type': 'person_without_user',
                    'person_uuid': person_id,
                    'given_name': given_name,
                    'family_name': family_name,
                    'sk_person_id': sk_person_id,
                    'sk_email': sk_email,
                    'posts_count': len(posts),
                    'message': f"Person {given_name} {family_name} has no associated user account",
                    'remediation_attempted': False,
                    'remediation_reason': "Automatic user creation not implemented"
                })
                continue
            
            # Issue: User with wrong access level
            if has_data_owner_role and user.get('accessLevel') not in ['EDITOR', 'ADMIN']:
                check_results['issues'].append({
                    'type': 'user_wrong_access_level',
                    'person_uuid': person_id,
                    'given_name': given_name,
                    'family_name': family_name,
                    'user_email': user.get('email'),
                    'current_level': user.get('accessLevel'),
                    'required_level': 'EDITOR',
                    'message': f"User {user.get('email')} needs EDITOR access (current: {user.get('accessLevel')})",
                    'remediation_attempted': False,
                    'remediation_reason': "Automatic access level change not implemented"
                })
            
            # Issue: User without isPerson set correctly
            if user.get('isPerson') != person_name_format:
                check_results['issues'].append({
                    'type': 'user_wrong_person_link',
                    'person_uuid': person_id,
                    'given_name': given_name,
                    'family_name': family_name,
                    'user_email': user.get('email'),
                    'current_isperson': user.get('isPerson'),
                    'required_isperson': person_name_format,
                    'message': f"User {user.get('email')} has incorrect isPerson field",
                    'remediation_attempted': False,
                    'remediation_reason': "Automatic isPerson update not implemented"
                })
        
        # Update check results based on issues found
        if len(check_results['issues']) == 0:
            check_results['status'] = 'success'
            check_results['message'] = "All persons with sk_person_id have correct user accounts."
        else:
            check_results['status'] = 'warning'
            check_results['message'] = f"Found {len(check_results['issues'])} issues with user accounts."
            
    except Exception as e:
        # Capture error information
        error_message = str(e)
        error_traceback = traceback.format_exc()
        logging.error(f"Exception occurred during check: {error_message}")
        logging.error(f"Traceback: {error_traceback}")
        
        # Update the check_results with error status
        check_results['status'] = 'error'
        check_results['message'] = f"User account check failed. Error: {error_message}."
        check_results['error'] = error_traceback
    
    finally:
        # Log a brief summary
        logging.info("")
        logging.info(f"User account check - Status: {check_results['status']}")
        logging.info(f"User account check - Message: {check_results['message']}")
        
        if check_results['issues']:
            logging.info(f"User account check - Found {len(check_results['issues'])} issues")
        
        return check_results


if __name__ == '__main__':
    logging.basicConfig(
        level=logging.INFO
    )
    logging.info(f"=== CURRENT DATABASE: {config.database_name} ===")
    logging.info(f'Executing {__file__}...')
    
    # Initialize client for database access
    dataspot_base_client = BaseDataspotClient(
        base_url=config.base_url, 
        database_name=config.database_name,
        scheme_name='NOT_IN_USE', 
        scheme_name_short='NotFound404'
    )
    
    # Run the check
    results = check_user_accounts(dataspot_client=dataspot_base_client)
    
    # Display results
    if results['issues']:
        logging.info(f"Found {len(results['issues'])} issues that need attention")
        for issue in results['issues']:
            logging.info(f"- {issue['message']}")
    else:
        logging.info("All user accounts are correctly configured!")
    
    logging.info("")
    logging.info("Execution completed.")
