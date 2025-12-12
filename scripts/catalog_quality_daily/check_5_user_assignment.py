import logging
from typing import Dict, List, Any

import config
from src.common import requests_get, requests_patch, requests_post
from src.clients.base_client import BaseDataspotClient
from src.staatskalender_auth import StaatskalenderAuth


def check_5_user_assignment(dataspot_client: BaseDataspotClient, staatskalender_person_email_cache: Dict[str, str] = None) -> Dict[str, any]:
    """
    Check #5: Benutzerkontensynchronisation
    
    This check verifies that all persons with sk_person_id and posts have correct user accounts.
    
    Specifically:
    - For all persons with sk_person_id and a post, it checks:
      - A user with the correct email address from Staatskalender exists
      - The user is correctly linked to the person via the isPerson field
      - The user has at least EDITOR access rights
    
    Remediation steps:
    - If a person has no email address in Staatskalender, this is reported without making changes
    - If no user exists for the person, a user is automatically created with:
      - The correct email address from Staatskalender
      - Proper link to the person via isPerson field in "lastname, firstname" format
      - EDITOR access rights
    - If a user exists but is not correctly linked to the person, the user is automatically linked
      to the correct person by updating the isPerson field
    - If the user has READ_ONLY access rights, they are automatically upgraded to EDITOR access level
    
    Args:
        dataspot_client: Base client for database operations
        staatskalender_person_email_cache: Cache of sk_person_id to email mappings from check_2
        
    Returns:
        dict: Check results including status, issues, and any errors
    """
    logging.debug("Starting Check #5: Benutzerkontensynchronisation...")
    
    result = {
        'status': 'success',
        'message': 'All persons with sk_person_id and posts have correct user assignments.',
        'issues': [],
        'error': None
    }
    
    if not staatskalender_person_email_cache:
        staatskalender_person_email_cache = {}
        logging.warning("No person email cache provided, will need to retrieve email information")
    
    try:
        # Get all persons with sk_person_id and their post assignments
        persons_with_sk_id = get_persons_with_sk_person_id(dataspot_client)
        
        if not persons_with_sk_id:
            result['message'] = 'No persons with sk_person_id found.'
            return result
            
        logging.info(f"Found {len(persons_with_sk_id)} persons with sk_person_id to verify")
        
        # Initialize Staatskalender authentication
        staatskalender_auth = StaatskalenderAuth()
        
        # Get all users from Dataspot
        users = get_all_users(dataspot_client)
        logging.info(f"Found {len(users)} users in the system")
        
        # Create lookup dictionaries for faster access
        users_by_email = {}
        for user in users:
            if user['email']:
                # Always store and lookup with lowercase email
                users_by_email[user['email'].lower()] = user
        
        # Create a lookup dictionary for users by person UUID
        users_by_person_uuid = {}
        for user in users:
            person_uuid = user.get('linked_person_uuid')
            if person_uuid:
                users_by_person_uuid[person_uuid] = user
        
        # Process each person
        for person in persons_with_sk_id:
            person_uuid = person['person_uuid']
            sk_person_id = person['sk_person_id'].strip('"')
            given_name = person['given_name']
            family_name = person['family_name']
            person_name = f"{given_name} {family_name}"
            has_posts = person['posts_count'] > 0
            
            logging.debug(f"Processing person: {person_name} (UUID: {person_uuid}) - Has posts: {has_posts}")
            
            # Get email from cache or from Staatskalender
            email = staatskalender_person_email_cache.get(sk_person_id) if staatskalender_person_email_cache else None
            if not email:
                sk_details = get_person_details_from_staatskalender(sk_person_id, staatskalender_auth)
                email = sk_details.get('email')
                
                # Update person name if different from Staatskalender
                if sk_details and sk_details.get('first_name') and sk_details.get('last_name'):
                    sk_first_name = sk_details['first_name']
                    sk_last_name = sk_details['last_name']
                    
                    if sk_first_name != given_name or sk_last_name != family_name:
                        update_success = update_person_name(
                            dataspot_client=dataspot_client,
                            person_uuid=person_uuid,
                            first_name=sk_first_name,
                            last_name=sk_last_name
                        )
                        
                        # Log person name update result
                        if update_success:
                            # Log the update
                            issue_message = f"Updated person name from '{person_name}' to '{sk_first_name} {sk_last_name}'"
                            result['issues'].append({
                                'type': 'person_name_update',
                                'person_uuid': person_uuid,
                                'given_name': given_name,
                                'family_name': family_name,
                                'sk_first_name': sk_first_name,
                                'sk_last_name': sk_last_name,
                                'message': issue_message,
                                'remediation_attempted': True,
                                'remediation_success': True
                            })
                            logging.info(issue_message)
                            
                            # Update the local variables to use the new name in subsequent logs and operations
                            given_name = sk_first_name
                            family_name = sk_last_name
                            person_name = f"{given_name} {family_name}"
                        else:
                            issue_message = f"Failed to update person name from '{person_name}' to '{sk_first_name} {sk_last_name}'"
                            result['issues'].append({
                                'type': 'person_name_update_failed',
                                'person_uuid': person_uuid,
                                'given_name': given_name,
                                'family_name': family_name,
                                'sk_first_name': sk_first_name,
                                'sk_last_name': sk_last_name,
                                'message': issue_message,
                                'remediation_attempted': True,
                                'remediation_success': False
                            })
                            logging.info(issue_message)
            
            # Handle case where no email is available
            if not email:
                # Check if user already exists by person UUID (even without email)
                user = users_by_person_uuid.get(person_uuid)
                
                if not user and has_posts:
                    # Only create an issue if no user exists AND person has posts
                    issue_message = f"Person {person_name} has no email address in Staatskalender. " \
                                   f"Please add an email address in Staatskalender or manually create a user account."
                    result['issues'].append({
                        'type': 'person_mismatch_missing_email',
                        'person_uuid': person_uuid,
                        'given_name': given_name,
                        'family_name': family_name,
                        'sk_person_id': sk_person_id,
                        'posts_count': person['posts_count'],
                        'message': issue_message,
                        'remediation_attempted': False,
                        'remediation_success': False
                    })
                    logging.info(issue_message)
                elif user:
                    # User exists but no email - this is actually fine, just log it
                    logging.debug(f"Person {person_name} has a user account but no email in Staatskalender - this is acceptable")
                else:
                    # No user and no posts - not an issue
                    logging.debug(f"Person {person_name} has no posts and no email in Staatskalender - no user account needed")
                
                # Skip to next person since we can't create a user without an email
                continue
            
            # Step 1: Check if user exists by email (primary method)
            user = users_by_email.get(email.lower())
            
            # Step 2: If not found by email, check if linked to person by UUID
            if not user:
                user = users_by_person_uuid.get(person_uuid)
            
            # Step 3: If still no user and person has posts, create one
            if not user:
                if has_posts:
                    logging.info(f"Person {person_name} (UUID: {person_uuid}) has posts but no associated user account - creating one")
                    
                    create_result = create_user_for_person(
                        dataspot_client=dataspot_client,
                        email=email,
                        given_name=given_name,
                        family_name=family_name,
                        person_uuid=person_uuid,
                        has_posts=has_posts
                    )
                    
                    if create_result['success']:
                        issue_message = f"Successfully created user account for {person_name} with email {email}"
                        result['issues'].append({
                            'type': 'user_created',
                            'person_uuid': person_uuid,
                            'given_name': given_name,
                            'family_name': family_name,
                            'sk_person_id': sk_person_id,
                            'posts_count': person['posts_count'],
                            'user_uuid': create_result['user_uuid'],
                            'user_email': email,
                            'user_access_level': create_result['access_level'],
                            'message': issue_message,
                            'remediation_attempted': True,
                            'remediation_success': True
                        })
                        logging.info(issue_message)
                    else:
                        issue_message = f"Failed to create user account for {person_name} with email {email}: {create_result['message']}"
                        result['issues'].append({
                            'type': 'user_creation_failed',
                            'person_uuid': person_uuid,
                            'given_name': given_name,
                            'family_name': family_name,
                            'sk_person_id': sk_person_id,
                            'posts_count': person['posts_count'],
                            'user_email': email,
                            'message': issue_message,
                            'remediation_attempted': True,
                            'remediation_success': False
                        })
                        logging.info(issue_message)
                else:
                    # Person has no posts, no need to create a user
                    logging.debug(f"Person {person_name} (UUID: {person_uuid}) has no posts - not creating user account")
                continue
            
            # Step 4: User exists but may not be correctly linked to the person
            if user['linked_person_uuid'] != person_uuid:
                logging.info(f"User {user['email']} exists but is not linked to person {person_name} - fixing link")
                
                # Get user API endpoint
                user_uuid = user['user_uuid']
                api_url = f"{dataspot_client.base_url}/rest/{dataspot_client.database_name}/users/{user_uuid}"
                
                # Build the update payload with isPerson in "lastname, firstname" format
                payload = {
                    "_type": "Benutzer",
                    "isPerson": f"{family_name}, {given_name}"
                }
                
                # Send API request to update the user
                try:
                    response = requests_patch(url=api_url, json=payload, headers=dataspot_client.auth.get_headers())
                    
                    if response.status_code == 200:
                        logging.info(f"Successfully linked user {user['email']} to person {person_name}")
                        issue_message = f"User {user['email']} is now correctly linked to person {person_name}"
                        result['issues'].append({
                            'type': 'user_person_link_updated',
                            'person_uuid': person_uuid,
                            'given_name': given_name,
                            'family_name': family_name,
                            'sk_person_id': sk_person_id,
                            'user_uuid': user_uuid,
                            'user_email': user['email'],
                            'message': issue_message,
                            'remediation_attempted': True,
                            'remediation_success': True
                        })
                        logging.info(issue_message)
                    else:
                        logging.error(f"Failed to link user to person. Status code: {response.status_code}")
                        logging.error(f"Response: {response.text}")
                        issue_message = f"Failed to link user {user['email']} to person {person_name}"
                        result['issues'].append({
                            'type': 'user_person_link_update_failed',
                            'person_uuid': person_uuid,
                            'given_name': given_name,
                            'family_name': family_name,
                            'sk_person_id': sk_person_id,
                            'user_uuid': user_uuid,
                            'user_email': user['email'],
                            'message': issue_message,
                            'remediation_attempted': True,
                            'remediation_success': False
                        })
                        logging.info(issue_message)
                except Exception as e:
                    logging.error(f"Exception while linking user to person: {str(e)}", exc_info=True)
                    issue_message = f"Exception while linking user {user['email']} to person {person_name}: {str(e)}"
                    result['issues'].append({
                        'type': 'user_person_link_update_failed',
                        'person_uuid': person_uuid,
                        'given_name': given_name,
                        'family_name': family_name,
                        'sk_person_id': sk_person_id,
                        'user_uuid': user_uuid,
                        'user_email': user['email'],
                        'message': issue_message,
                        'remediation_attempted': True,
                        'remediation_success': False
                    })
                    logging.info(issue_message)
            
            # Step 5: Check if user has correct access level (if person has posts)
            if has_posts and user['access_level'] == 'READ_ONLY':
                logging.debug(f"User {user['email']} has READ_ONLY access but person {person_name} has posts - upgrading to EDITOR")
                
                update_success = update_user_access_level(
                    dataspot_client=dataspot_client,
                    user_uuid=user['user_uuid'],
                    access_level='EDITOR'
                )
                
                if update_success:
                    issue_message = f"Updated access level for user {user['email']} from READ_ONLY to EDITOR"
                    result['issues'].append({
                        'type': 'access_level_updated',
                        'person_uuid': person_uuid,
                        'given_name': given_name,
                        'family_name': family_name,
                        'sk_person_id': sk_person_id,
                        'user_uuid': user['user_uuid'],
                        'user_email': user['email'],
                        'user_access_level_old': user['access_level'],
                        'user_access_level_new': ['EDITOR'],
                        'message': issue_message,
                        'remediation_attempted': True,
                        'remediation_success': True
                    })
                    logging.info(f"Successfully updated user access level from READ_ONLY to EDITOR for {user['email']} (Person {given_name} {family_name})")
                    logging.info(issue_message)
                else:
                    issue_message = f"Failed to update access level for user {user['email']} from READ_ONLY to EDITOR"
                    result['issues'].append({
                        'type': 'access_level_update_failed',
                        'person_uuid': person_uuid,
                        'given_name': given_name,
                        'family_name': family_name,
                        'sk_person_id': sk_person_id,
                        'user_uuid': user['user_uuid'],
                        'user_email': user['email'],
                        'user_access_level': user['access_level'],
                        'message': issue_message,
                        'remediation_attempted': True,
                        'remediation_success': False
                    })
                    logging.error(f"Failed to update access level from READ_ONLY to EDITOR for user {user['email']} (Person {given_name} {family_name})")
                    logging.info(issue_message)
        
        # Update final status and message
        if result['issues']:
            issue_count = len(result['issues'])
            result['status'] = 'warning'
            result['message'] = f"Check #5: Found {issue_count} issues with user assignments"
            logging.info(f"Check finished: Found {issue_count} issues with user assignments")
        else:
            logging.info("Check finished: All persons with posts have correct user assignments")
    
    except Exception as e:
        result['status'] = 'error'
        result['error'] = str(e)
        result['message'] = f"Error in Check #5 (Benutzerkontensynchronisation): {str(e)}"
        logging.error(f"Error in Check #5 (Benutzerkontensynchronisation): {str(e)}", exc_info=True)
    
    return result


def get_persons_with_sk_person_id(dataspot_client: BaseDataspotClient) -> List[Dict[str, any]]:
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


def get_person_details_from_staatskalender(sk_person_id: str, staatskalender_auth: StaatskalenderAuth) -> Dict[str, Any]:
    """
    Retrieve person details from Staatskalender by sk_person_id.
    
    Args:
        sk_person_id: Staatskalender person ID
        staatskalender_auth: Authentication object for Staatskalender API
        
    Returns:
        dict: Person details including first_name, last_name, email or empty dict if error
    """
    logging.debug(f"Retrieving person details from Staatskalender for person with SK ID: {sk_person_id}")
    
    # Add a delay to prevent overwhelming the API
    import time
    time.sleep(1)
    
    person_url = f"https://staatskalender.bs.ch/api/people/{sk_person_id}"
    try:
        person_response = requests_get(url=person_url, auth=staatskalender_auth.get_auth())
        
        if person_response.status_code != 200:
            logging.warning(f"Failed to retrieve person data from Staatskalender. Status code: {person_response.status_code}")
            return {}
            
        # Extract person details
        person_data = person_response.json()
        sk_email = None
        sk_first_name = None
        sk_last_name = None
        
        for item in person_data.get('collection', {}).get('items', []):
            for data_item in item.get('data', []):
                if data_item.get('name') == 'email':
                    sk_email = data_item.get('value')
                elif data_item.get('name') == 'first_name':
                    # Replace spaces with underscores as spaces in names are not allowed in dataspot!
                    raw_first_name = data_item.get('value')
                    if raw_first_name:
                        sk_first_name = raw_first_name.strip().replace(' ', '_') if raw_first_name.strip() else None
                        # Log if we had to replace spaces
                        if raw_first_name.strip() and ' ' in raw_first_name:
                            logging.debug(f'   - Replaced spaces in first name: "{raw_first_name}" -> "{sk_first_name}"')
                elif data_item.get('name') == 'last_name':
                    sk_last_name = data_item.get('value')
                    if sk_last_name:
                        sk_last_name = sk_last_name.strip() if sk_last_name.strip() else None

                if sk_email and sk_first_name and sk_last_name:
                    break

        if sk_email:
            logging.debug(f"Found email in Staatskalender: {sk_email}")
        else:
            logging.debug(f"No email found in Staatskalender for this person")
            
        if sk_first_name and sk_last_name:
            logging.debug(f"Found name in Staatskalender: {sk_first_name} {sk_last_name}")
            
        return {
            'first_name': sk_first_name,
            'last_name': sk_last_name,
            'email': sk_email
        }
        
    except Exception as e:
        logging.error(f"Error retrieving person data from Staatskalender: {str(e)}")
        return {}


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
        u.is_person AS linked_person_uuid
    FROM
        user_view u
    WHERE 
        u.service_user IS NULL OR u.service_user = false
    ORDER BY
        u.login_id
    """
    return dataspot_client.execute_query_api(sql_query=query)


def update_person_name(dataspot_client: BaseDataspotClient, person_uuid: str, first_name: str, last_name: str) -> bool:
    """
    Update a person's name in Dataspot to match the name in Staatskalender.
    
    This function is part of the data consistency maintenance process. While not directly
    mentioned in the check description, it ensures that person names are kept in sync with 
    the Staatskalender, which is considered the authoritative source of information.
    
    Args:
        dataspot_client: Base client for database operations
        person_uuid: UUID of the person to update
        first_name: New first name from Staatskalender
        last_name: New last name from Staatskalender
        
    Returns:
        bool: True if update was successful, False otherwise
    """
    logging.debug(f"Updating person name for UUID {person_uuid} to {first_name} {last_name}")
    
    # Construct the API URL to update person
    api_url = f"{dataspot_client.base_url}/rest/{dataspot_client.database_name}/persons/{person_uuid}"
    
    # Build the update payload
    payload = {
        "_type": "Person",
        "givenName": first_name,
        "familyName": last_name
    }
    
    try:
        # Send PATCH request to update person
        response = requests_patch(url=api_url, json=payload, headers=dataspot_client.auth.get_headers())
        
        if response.status_code == 200:
            logging.info(f"Successfully updated person name to {first_name} {last_name}")
            return True
        else:
            logging.error(f"Failed to update person name. Status code: {response.status_code}")
            logging.error(f"Response: {response.text}")
            return False
    except Exception as e:
        logging.error(f"Exception while updating person name: {str(e)}", exc_info=True)
        return False


def create_user_for_person(dataspot_client: BaseDataspotClient, email: str, given_name: str, family_name: str, person_uuid: str, has_posts: bool) -> dict:
    """
    Create a user account for a person and properly link them together.
    
    This function implements the remediation step of creating a user account when none exists.
    It automatically configures the user with:
    - The correct email address from Staatskalender
    - Proper link to the person via isPerson field in "lastname, firstname" format
    - EDITOR access rights if the person has posts, otherwise READ_ONLY
    
    Args:
        dataspot_client: Base client for database operations
        email: Email address for the new user
        given_name: First name of the person
        family_name: Last name of the person
        person_uuid: UUID of the person to link this user to
        has_posts: Whether the person has posts assigned (determines access level)
        
    Returns:
        dict: Result with success status and message
    """
    logging.debug(f"Creating new user for person {given_name} {family_name} with email {email}")
    
    # Construct the API URL to create user
    api_url = f"{dataspot_client.base_url}/rest/{dataspot_client.database_name}/users"
    
    # Determine access level based on whether person has posts
    access_level = "EDITOR" if has_posts else "READ_ONLY"
    
    # Build the user creation payload
    # isPerson field links to the person UUID
    # loginId must be lowercase per API requirement
    payload = {
        "_type": "Benutzer",
        "loginId": email.lower(),  # Convert to lowercase to avoid violations
        "isPerson": person_uuid,
        "accessLevel": access_level
    }
    
    result = {
        'success': False,
        'user_uuid': None,
        'message': '',
        'access_level': access_level
    }
    
    try:
        # Send POST request to create user
        response = requests_post(
            url=api_url,
            json=payload,
            headers=dataspot_client.auth.get_headers()
        )
        
        if response.status_code in [200, 201]:  # 200 OK or 201 Created
            user_data = response.json()
            user_uuid = user_data.get('id')
            result['success'] = True
            result['user_uuid'] = user_uuid
            result['message'] = f"Successfully created user {email} with access level {access_level}"
            logging.info(f"Successfully created user {email} for person {given_name} {family_name}")
                
            return result
        else:
            error_msg = f"Failed to create user. Status code: {response.status_code}"
            if hasattr(response, 'text'):
                error_msg += f", Response: {response.text}"
            logging.error(error_msg)
            result['message'] = error_msg
            return result
    except Exception as e:
        error_msg = f"Exception while creating user: {str(e)}"
        logging.error(error_msg, exc_info=True)
        result['message'] = error_msg
        return result


def update_user_access_level(dataspot_client: BaseDataspotClient, user_uuid: str, access_level: str) -> bool:
    """
    Update a user's access level in Dataspot.
    
    This function implements the remediation step of upgrading a user's access level
    from READ_ONLY to EDITOR when the person has posts. This ensures that users who
    need to manage data have the appropriate permissions.
    
    Args:
        dataspot_client: Base client for database operations
        user_uuid: UUID of the user to update
        access_level: Access level to set the user to (READ_ONLY, EDITOR, ADMINISTRATOR)
        
    Returns:
        bool: True if update was successful, False otherwise
    """
    logging.debug(f"Updating user access level for UUID {user_uuid} to {access_level}")
    
    # Construct the API URL to update user
    api_url = f"{dataspot_client.base_url}/rest/{dataspot_client.database_name}/users/{user_uuid}"
    
    # Build the update payload
    payload = {
        "_type": "Benutzer",
        "accessLevel": access_level
    }
    
    try:
        # Send PATCH request to update user
        response = requests_patch(url=api_url, json=payload, headers=dataspot_client.auth.get_headers())
        
        if response.status_code == 200:
            logging.info(f"Successfully updated user access level to {access_level}")
            return True
        else:
            logging.error(f"Failed to update user access level. Status code: {response.status_code}")
            logging.error(f"Response: {response.text}")
            return False
    except Exception as e:
        logging.error(f"Exception while updating user access level: {str(e)}", exc_info=True)
        return False