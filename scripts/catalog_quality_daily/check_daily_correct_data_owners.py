import logging
import traceback
from typing import Any, Dict, List, Optional

import config
from src.clients.base_client import BaseDataspotClient
from src.common import requests_get
import requests

from scripts.catalog_quality_daily.daily_checks_helpers import update_person_holdspost, set_user_access_level, update_user_is_person, build_users_by_email_mapping, build_persons_by_post_mapping, get_user_by_email

# THIS FILE IS ANYTHING BUT BEAUTIFUL, BUT IT WORKS!
# IF I EVER HAVE THE TIME, I WANT TO CLEAN THIS UP, INCLUDING daily_checks_helpers.py


def get_data_owner_posts(dataspot_client: BaseDataspotClient) -> Optional[List[Dict[str, Any]]]:
    """
    Execute query to find all Data Owner posts.
    
    Args:
        dataspot_client: The client for connecting to Dataspot API
        
    Returns:
        List of Data Owner posts or None if query fails
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
    
    if isinstance(result, list):
        return result
    else:
        logging.error(f"Invalid response format from the Query API: {type(result)}")
        return None


def get_post_details(dataspot_client: BaseDataspotClient, post_uuid: str, post_label: str) -> Dict[str, Any]:
    """
    Get details of a specific post from Dataspot API.
    
    Args:
        dataspot_client: The client for connecting to Dataspot API
        post_uuid: UUID of the post
        post_label: Label of the post (for logging)
        
    Returns:
        Dict containing:
        - 'success': Boolean indicating if the request was successful
        - 'data': Post data if successful, None otherwise
        - 'status_code': HTTP status code returned by the API
    """
    post_url = f"{config.base_url}/api/{config.database_name}/posts/{post_uuid}"
    
    post_response = requests_get(
        url=post_url,
        headers=dataspot_client.auth.get_headers()
    )
    
    # Check if we got valid post data
    if post_response.status_code != 200:
        logging.warning(f"Could not retrieve post data for '{post_label}' (UUID: {post_uuid}). Status code: {post_response.status_code}")
        return {
            'success': False,
            'data': None,
            'status_code': post_response.status_code
        }
        
    return {
        'success': True,
        'data': post_response.json().get('asset', {}),
        'status_code': post_response.status_code
    }


def get_membership_details(membership_id: str) -> Dict[str, Any]:
    """
    Get membership details from Staatskalender API.
    
    Args:
        membership_id: ID of the membership to retrieve
        
    Returns:
        Dict containing:
        - 'success': Boolean indicating if the request was successful
        - 'data': Membership data if successful, None otherwise
        - 'status_code': HTTP status code returned by the API
    """
    membership_url = f"https://staatskalender.bs.ch/api/memberships/{membership_id}"
    logging.info(f"Retrieving membership data from Staatskalender: {membership_url}")
    
    membership_response = requests.get(url=membership_url)
    
    if membership_response.status_code != 200:
        logging.warning(f"Invalid membership_id '{membership_id}'. Status code: {membership_response.status_code}")
        return {
            'success': False,
            'data': None,
            'status_code': membership_response.status_code
        }
        
    return {
        'success': True,
        'data': membership_response.json(),
        'status_code': membership_response.status_code
    }


def get_person_link_from_membership(membership_data: Dict[str, Any]) -> Optional[str]:
    """
    Extract person link from membership data.
    
    Args:
        membership_data: Membership data from Staatskalender
        
    Returns:
        URL to person details or None if not found
    """
    for item in membership_data.get('collection', {}).get('items', []):
        for link in item.get('links', []):
            if link.get('rel') == 'person':
                return link.get('href')
    
    return None


def get_person_details_from_staatskalender(person_link: str) -> Dict[str, Any]:
    """
    Get person details from Staatskalender API.
    
    Args:
        person_link: URL to person details in Staatskalender
        
    Returns:
        Dict containing:
        - 'success': Boolean indicating if the request was successful
        - 'data': Person data dict with first_name, last_name, and email if successful
        - 'status_code': HTTP status code returned by the API
    """
    logging.info(f"Retrieving person data from Staatskalender: {person_link}")
    person_response = requests_get(url=person_link, headers={})  # No dataspot headers needed
    
    if person_response.status_code != 200:
        logging.warning(f"Could not retrieve person data from Staatskalender. Status code: {person_response.status_code}")
        return {
            'success': False,
            'data': None,
            'status_code': person_response.status_code
        }
    
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
        'success': True,
        'data': {
            'first_name': first_name,
            'last_name': last_name,
            'email': email
        },
        'status_code': person_response.status_code
    }

def check_correct_data_owners(dataspot_client: BaseDataspotClient) -> Dict[str, Any]:
    """
    Check if all Data Owner posts are assigned to the correct person according to Staatskalender.

    This method:
    1. Executes a SQL query to find all Data Owner posts
    2. For each post:
       - Checks if it has a membership_id
       - Verifies the membership exists in Staatskalender
       - Checks if it has exactly one person assigned to it in Dataspot
       - Compares the person in Dataspot with the person in Staatskalender
       - Verifies that the person in Staatskalender has an email address
       - Checks if a user with that email address exists in Dataspot
       - Verifies that the user has the correct access level (EDITOR)
       - Confirms that the user is connected to the correct person in Dataspot
    3. Logs the results and generates a report
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
        result = get_data_owner_posts(dataspot_client)

        # Process results
        if result:
            data_owner_posts = result
            post_count = len(data_owner_posts)

            logging.info(f"Found {post_count} Data Owner posts to check")

            # Build the mappings once at the beginning
            logging.info("Fetching all persons data to build post-person mapping...")
            persons_by_post = build_persons_by_post_mapping(dataspot_client)

            logging.info("Fetching all users data to build email-user mapping...")
            users_by_email = build_users_by_email_mapping(dataspot_client)

            # Examine each post
            for idx, post in enumerate(data_owner_posts):
                post_uuid = post.get('uuid')
                post_label = post.get('post_label', 'Unknown')

                logging.info(f"[{idx +1}/{post_count}] Checking Data Owner post: {post_label}")

                # Step 1: Get post details from Dataspot
                post_result = get_post_details(dataspot_client, post_uuid, post_label)
                
                if not post_result['success']:
                    issue = {
                        'type': 'error',
                        'post_uuid': post_uuid,
                        'post_label': post_label,
                        'message': f"Could not retrieve post data from Dataspot. Status code: {post_result['status_code']}"
                    }
                    check_results['issues'].append(issue)
                    continue
                
                post_data = post_result['data']

                # Step 2: Check if post has membership_id
                membership_id = post_data.get('customProperties', {}).get('membership_id')
                if not membership_id:
                    # Log the issue immediately
                    logging.warning(f"MISSING MEMBERSHIP: {dataspot_client.base_url}/web/{dataspot_client.database_name}/posts/{post_uuid}")

                    issue = {
                        'type': 'missing_membership',
                        'post_uuid': post_uuid,
                        'post_label': post_label,
                        'message': f"Post does not have a membership_id"
                    }
                    check_results['issues'].append(issue)
                    continue

                # Step 3: Check if membership exists in Staatskalender
                logging.info(f"Checking membership_id {membership_id} in Staatskalender...")
                membership_result = get_membership_details(membership_id)
                
                if not membership_result['success']:
                    # Log the invalid membership ID immediately
                    membership_url = f"https://staatskalender.bs.ch/api/memberships/{membership_id}"
                    logging.warning \
                        (f"INVALID MEMBERSHIP: Post '{post_label}' (UUID: {post_uuid}) has invalid membership_id '{membership_id}'. Status code: {membership_result['status_code']}")

                    issue = {
                        'type': 'invalid_membership',
                        'post_uuid': post_uuid,
                        'post_label': post_label,
                        'membership_id': membership_id,
                        'message': f"Membership ID not found in Staatskalender. Status code: {membership_result['status_code']}, invalid url: {membership_url}"
                    }
                    check_results['issues'].append(issue)
                    continue
                
                membership_data = membership_result['data']

                # Extract person link from membership data
                person_link = get_person_link_from_membership(membership_data)
                    
                if not person_link:
                    issue = {
                        'type': 'missing_person_link',
                        'post_uuid': post_uuid,
                        'post_label': post_label,
                        'membership_id': membership_id,
                        'message': f"Could not find person link in membership data"
                    }
                    check_results['issues'].append(issue)
                    continue

                # Step 4: Get person data from Staatskalender
                person_result = get_person_details_from_staatskalender(person_link)
                
                if not person_result['success']:
                    issue = {
                        'type': 'person_data_error',
                        'post_uuid': post_uuid,
                        'post_label': post_label,
                        'membership_id': membership_id,
                        'message': f"Could not retrieve person data from Staatskalender. Status code: {person_result['status_code']}"
                    }
                    check_results['issues'].append(issue)
                    continue
                
                # Extract person details
                person_data = person_result['data']
                sk_first_name = person_data['first_name']
                sk_last_name = person_data['last_name']
                sk_email = person_data['email']
                
                if not sk_first_name or not sk_last_name:
                    issue = {
                        'type': 'missing_person_name',
                        'post_uuid': post_uuid,
                        'post_label': post_label,
                        'membership_id': membership_id,
                        'message': f"Could not extract name from Staatskalender person data"
                    }
                    check_results['issues'].append(issue)
                    continue

                # Step 5: Find associated person in Dataspot using the cached persons data
                # Find person for this post from our pre-built mapping
                dataspot_persons_info = persons_by_post.get(post_uuid, [])

                # Check if no person is assigned to this Data Owner post
                if not dataspot_persons_info:
                    # Attempt to remediate if we have email from Staatskalender
                    if sk_email:
                        remediation_success = False
                        remediation_steps = []
                        error_message = None

                        try:
                            logging.info(f"Attempting to assign a person to post {post_label}")

                            # 1. Create person if needed (with name from Staatskalender)
                            person_uuid, person_newly_created = dataspot_client.ensure_person_exists(sk_first_name, sk_last_name)

                            if person_newly_created:
                                remediation_steps.append("created_new_person")
                                logging.info(f"Created new person: {sk_first_name} {sk_last_name} with ID: {person_uuid}")
                            else:
                                remediation_steps.append("found_existing_person")
                                logging.info(f"Found existing person: {sk_first_name} {sk_last_name} with ID: {person_uuid}")

                            # 2. Add Data Owner Post to the person
                            success, _ = update_person_holdspost(dataspot_client, person_uuid, post_uuid, add=True)
                            if success:
                                remediation_steps.append("added_post_to_person")
                                # Update our cached mapping since we modified it
                                if post_uuid not in persons_by_post:
                                    persons_by_post[post_uuid] = []
                                
                                # Add the person to the cached mapping
                                person_info = {
                                    'uuid': person_uuid,
                                    'given_name': sk_first_name,
                                    'family_name': sk_last_name
                                }
                                persons_by_post[post_uuid].append(person_info)

                                # 3. Check if there's already a user with the Staatskalender email
                                if sk_email in users_by_email:
                                    user = users_by_email[sk_email]
                                    logging.info(f"Found existing user with email {sk_email}, ID: {user.get('id')}")
                                    expected_is_person = f"{sk_last_name}, {sk_first_name}"

                                    if user.get('isPerson') != expected_is_person:
                                        logging.info(f"User isPerson field needs update from '{user.get('isPerson')}' to '{expected_is_person}'")
                                        if update_user_is_person(dataspot_client, sk_email, expected_is_person):
                                            remediation_steps.append("updated_user_is_person")
                                            logging.info(f"Successfully updated isPerson field for user {sk_email}")

                                    # Set user access level to EDITOR if needed
                                    if user.get('accessLevel') != "EDITOR":
                                        logging.info(f"User access level needs update from '{user.get('accessLevel')}' to 'EDITOR'")
                                        user_id = user.get('id')
                                        if user_id and set_user_access_level(dataspot_client, user_id, "EDITOR"):
                                            remediation_steps.append("set_user_access_to_editor")
                                            logging.info(f"Successfully updated access level for user {sk_email} to EDITOR")
                                else:
                                    logging.info(f"No user found with email {sk_email}, creating new user")
                                    # 4. Create user if it doesn't exist (and ensure it is linked to the correct person)
                                    user_uuid, user_newly_created = dataspot_client.ensure_user_exists(sk_email, person_uuid, access_level="EDITOR")
                                    if user_newly_created:
                                        remediation_steps.append("created_new_user")
                                        logging.info(f"Successfully created new user: {sk_email} with ID: {user_uuid}")

                                remediation_success = True
                                logging.info(f"Successfully assigned person to Data Owner post {post_label}")
                                
                                # After successful remediation, skip to the next post
                                # instead of adding this as an issue
                                issue = {
                                    'type': 'no_person_assigned',
                                    'post_uuid': post_uuid,
                                    'post_label': post_label,
                                    'membership_id': membership_id,
                                    'sk_first_name': sk_first_name,
                                    'sk_last_name': sk_last_name,
                                    'message': f"Successfully assigned person {sk_first_name} {sk_last_name} to post",
                                    'remediation_attempted': True,
                                    'remediation_success': True,
                                    'remediation_steps': remediation_steps,
                                    'remediation_error': None
                                }
                                check_results['issues'].append(issue)
                                continue
                        except Exception as e:
                            error_message = str(e)
                            logging.error(f"Error during remediation: {error_message}")

                        issue = {
                            'type': 'no_person_assigned',
                            'post_uuid': post_uuid,
                            'post_label': post_label,
                            'membership_id': membership_id,
                            'sk_first_name': sk_first_name,
                            'sk_last_name': sk_last_name,
                            'message': f"No person assigned to this post in Dataspot",
                            'remediation_attempted': True,
                            'remediation_success': remediation_success,
                            'remediation_steps': remediation_steps,
                            'remediation_error': error_message
                        }
                    else:
                        # No remediation possible without email
                        issue = {
                            'type': 'no_person_assigned',
                            'post_uuid': post_uuid,
                            'post_label': post_label,
                            'membership_id': membership_id,
                            'sk_first_name': sk_first_name,
                            'sk_last_name': sk_last_name,
                            'message': f"No person assigned to this post in Dataspot",
                            'remediation_attempted': False,
                            'remediation_reason': "No email available from Staatskalender"
                        }
                    check_results['issues'].append(issue)
                    continue

                # Check if multiple persons are assigned to this Data Owner post
                if len(dataspot_persons_info) > 1:
                    person_names = [f"{p.get('given_name', '')} {p.get('family_name', '')}" for p in dataspot_persons_info]

                    issue = {
                        'type': 'multiple_persons_assigned',
                        'post_uuid': post_uuid,
                        'post_label': post_label,
                        'membership_id': membership_id,
                        'sk_first_name': sk_first_name,
                        'sk_last_name': sk_last_name,
                        'dataspot_persons': person_names,
                        'message': f"Multiple persons assigned to this Data Owner post: {', '.join(person_names)}"
                    }
                    check_results['issues'].append(issue)
                    continue

                # Extract first person's details for comparison
                dataspot_person_info = dataspot_persons_info[0]
                dataspot_first_name = dataspot_person_info.get('given_name')
                dataspot_last_name = dataspot_person_info.get('family_name')
                dataspot_person_uuid = dataspot_person_info.get('uuid')

                # Check for missing name information in Dataspot person
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
                    continue

                # Check if correct person is assigned to post
                if sk_first_name != dataspot_first_name or sk_last_name != dataspot_last_name:
                    # Attempt remediation regardless of email availability
                    logging.info(f"Attempting to remediate person mismatch for post {post_label}")
                    remediation_success = False
                    remediation_steps = []
                    error_message = None
                    person_uuid = None
                    has_remaining_posts = False

                    try:
                        # Get the current user email for potentially downgrading access, if available
                        current_user_email = None
                        if dataspot_person_uuid:  # Only look for current user if there's a current person
                            for email, user in users_by_email.items():
                                # Look for a user with isPerson matching the dataspot person
                                expected_is_person = f"{dataspot_last_name}, {dataspot_first_name}"
                                if user.get('isPerson') == expected_is_person:
                                    current_user_email = email
                                    break

                        # 1. Remove the Data Owner Post from the current person if one exists
                        if dataspot_person_uuid:
                            success, has_remaining_posts = update_person_holdspost(dataspot_client, dataspot_person_uuid, post_uuid, add=False)
                            if success:
                                remediation_steps.append("removed_data_owner_post")
                                
                                # 2. If email available and the person now doesn't hold any posts, set access level to READ_ONLY
                                if sk_email and current_user_email and not has_remaining_posts:
                                    logging.info(f"Person {dataspot_person_uuid} has no remaining posts, setting user access to READ_ONLY")
                                    current_user_id = users_by_email.get(current_user_email, {}).get('id')
                                    if current_user_id and set_user_access_level(dataspot_client, current_user_id, "READ_ONLY"):
                                        remediation_steps.append("set_user_access_to_read_only")
                        else:
                            # No person is currently assigned, so nothing to remove
                            remediation_steps.append("no_person_to_remove")

                        # 3. Create new person if needed (with name from Staatskalender)
                        person_uuid, person_newly_created = dataspot_client.ensure_person_exists(sk_first_name, sk_last_name)

                        if person_newly_created:
                            remediation_steps.append("created_new_person")
                            logging.info(f"Created new person: {sk_first_name} {sk_last_name} with ID: {person_uuid}")

                        # 4. Add Data Owner Post to the correct person
                        success, _ = update_person_holdspost(dataspot_client, person_uuid, post_uuid, add=True)
                        if success:
                            remediation_steps.append("added_post_to_person")
                            
                            # Update our cached mapping since we modified it
                            if post_uuid not in persons_by_post:
                                persons_by_post[post_uuid] = []
                            else:
                                # Clear existing entries since we're replacing them
                                persons_by_post[post_uuid] = []
                            
                            # Add the person to the cached mapping
                            person_info = {
                                'uuid': person_uuid,
                                'given_name': sk_first_name,
                                'family_name': sk_last_name
                            }
                            persons_by_post[post_uuid].append(person_info)
                            
                            # Person has been successfully created and post assigned
                            # Now check if email is available for user creation/updates
                            if sk_email:
                                # Check if there's already a user with the Staatskalender email
                                if sk_email in users_by_email:
                                    # 5. If the isPerson field is incorrect, fix it
                                    user = users_by_email[sk_email]
                                    expected_is_person = f"{sk_last_name}, {sk_first_name}"

                                    if user.get('isPerson') != expected_is_person:
                                        if update_user_is_person(dataspot_client, sk_email, expected_is_person):
                                            remediation_steps.append("updated_user_is_person")

                                    # 6. If the user access level is not EDITOR, set it
                                    if user.get('accessLevel') != "EDITOR":
                                        user_id = user.get('id')
                                        if user_id and set_user_access_level(dataspot_client, user_id, "EDITOR"):
                                            remediation_steps.append("set_user_access_to_editor")
                                else:
                                    # 7. Create user if it doesn't exist (and ensure it is linked to the correct person)
                                    user_uuid, user_newly_created = dataspot_client.ensure_user_exists(sk_email, person_uuid, access_level="EDITOR")
                                    if user_newly_created:
                                        # Verify user was created with correct properties
                                        user_check = get_user_by_email(dataspot_client, sk_email)
                                        if not user_check or user_check.get('accessLevel') != "EDITOR":
                                            logging.error(f"User creation succeeded but properties incorrect")
                                        remediation_steps.append("created_new_user")
                                
                                remediation_success = True
                                logging.info(f"Successfully remediated Data Owner post {post_label}")
                                
                                # After successful remediation with email, skip to the next post
                                issue = {
                                    'type': 'person_mismatch',
                                    'post_uuid': post_uuid,
                                    'post_label': post_label,
                                    'membership_id': membership_id,
                                    'dataspot_person_uuid': dataspot_person_uuid,
                                    'sk_first_name': sk_first_name,
                                    'sk_last_name': sk_last_name,
                                    'dataspot_first_name': dataspot_first_name,
                                    'dataspot_last_name': dataspot_last_name,
                                    'message': f"Successfully reassigned post from {dataspot_first_name} {dataspot_last_name} to {sk_first_name} {sk_last_name}",
                                    'remediation_attempted': True,
                                    'remediation_success': True,
                                    'remediation_steps': remediation_steps,
                                    'remediation_error': None
                                }
                                check_results['issues'].append(issue)
                                continue
                            else:
                                # No email available - person created and post assigned, but user needs to be created manually
                                remediation_success = True
                                logging.info(f"Partially remediated Data Owner post {post_label} - person created but no user (email missing)")
                                
                                issue = {
                                    'type': 'person_mismatch_missing_email',
                                    'post_uuid': post_uuid,
                                    'post_label': post_label,
                                    'membership_id': membership_id,
                                    'dataspot_person_uuid': dataspot_person_uuid,
                                    'sk_first_name': sk_first_name,
                                    'sk_last_name': sk_last_name,
                                    'dataspot_first_name': dataspot_first_name,
                                    'dataspot_last_name': dataspot_last_name,
                                    'message': f"Person created but USER CREATION REQUIRED: {sk_first_name} {sk_last_name} has no email in Staatskalender. Person was created in Dataspot and assigned to post, but a user must be created manually.",
                                    'remediation_attempted': True,
                                    'remediation_success': remediation_success,
                                    'remediation_steps': remediation_steps,
                                    'remediation_error': error_message
                                }
                                check_results['issues'].append(issue)
                                continue
                    except Exception as e:
                        error_message = str(e)
                        logging.error(f"Error during remediation: {error_message}")
                        
                        # Determine the issue type based on whether email is available
                        issue_type = 'person_mismatch' if sk_email else 'person_mismatch_missing_email'
                        message = f"Person mismatch: Staatskalender ({sk_first_name} {sk_last_name}) vs. Dataspot ({dataspot_first_name} {dataspot_last_name})"
                        
                        if not sk_email:
                            message = f"Person mismatch and missing email: {sk_first_name} {sk_last_name} has no email in Staatskalender. Remediation failed: {error_message}"
                        
                        issue = {
                            'type': issue_type,
                            'post_uuid': post_uuid,
                            'post_label': post_label,
                            'membership_id': membership_id,
                            'dataspot_person_uuid': dataspot_person_uuid,
                            'sk_first_name': sk_first_name,
                            'sk_last_name': sk_last_name,
                            'dataspot_first_name': dataspot_first_name,
                            'dataspot_last_name': dataspot_last_name,
                            'message': message,
                            'remediation_attempted': True,
                            'remediation_success': remediation_success,
                            'remediation_steps': remediation_steps,
                            'remediation_error': error_message
                        }
                        check_results['issues'].append(issue)
                else:
                    logging.info \
                        (f"✓ Data Owner post '{post_label}' has correct person assignment: {dataspot_first_name} {dataspot_last_name}")

                    # Step 6: Check if the user has the same email as the person in Staatskalender
                    # First, check if we have an email from Staatskalender
                    if not sk_email:
                        issue = {
                            'type': 'missing_sk_email',
                            'post_uuid': post_uuid,
                            'post_label': post_label,
                            'membership_id': membership_id,
                            'dataspot_person_uuid': dataspot_person_uuid,
                            'message': f"Person in Staatskalender has no email address"
                        }
                        check_results['issues'].append(issue)
                        continue

                    # Check if any user is linked to this person via email
                    user_found = False
                    if sk_email in users_by_email:
                        user = users_by_email[sk_email]
                        user_found = True

                        # Check if the accessLevel is "EDITOR"
                        if user.get('accessLevel') != "EDITOR":
                            # Try to remediate by updating the user's access level
                            remediation_success = False
                            remediation_steps = []
                            error_message = None

                            try:
                                logging.info(f"Attempting to fix access level for user {sk_email}")

                                # Update user access level to EDITOR
                                user_id = user.get('id')
                                if user_id and set_user_access_level(dataspot_client, user_id, "EDITOR"):
                                    remediation_steps.append("set_user_access_to_editor")
                                    remediation_success = True
                                    logging.info(f"Successfully updated access level for user {sk_email}")
                            except Exception as e:
                                error_message = str(e)
                                logging.error(f"Error during remediation: {error_message}")

                            issue = {
                                'type': 'wrong_access_level',
                                'post_uuid': post_uuid,
                                'post_label': post_label,
                                'membership_id': membership_id,
                                'dataspot_person_uuid': dataspot_person_uuid,
                                'user_email': sk_email,
                                'current_access_level': user.get('accessLevel'),
                                'message': f"User {sk_email} has incorrect access level: {user.get('accessLevel')} (should be EDITOR)",
                                'remediation_attempted': True,
                                'remediation_success': remediation_success,
                                'remediation_steps': remediation_steps,
                                'remediation_error': error_message
                            }
                            check_results['issues'].append(issue)
                        else:
                            logging.info(f"✓ User {sk_email} has correct access level (EDITOR)")

                        # Check if isPerson field matches "{last name}, {first name}" format
                        expected_is_person = f"{dataspot_last_name}, {dataspot_first_name}"
                        actual_is_person = user.get('isPerson')
                        if actual_is_person != expected_is_person:
                            logging.info(f"✗ User {sk_email} has INCORRECT isPerson field: '{actual_is_person}' (should be '{expected_is_person}')")

                            # Try to remediate by updating the user's isPerson field
                            remediation_success = False
                            remediation_steps = []
                            error_message = None

                            try:
                                logging.info(f"Attempting to fix isPerson field for user {sk_email}")

                                # Update isPerson field
                                if update_user_is_person(dataspot_client, sk_email, expected_is_person):
                                    remediation_steps.append("updated_is_person_field")
                                    remediation_success = True
                                    logging.info(f"Successfully updated isPerson field for user {sk_email}")
                            except Exception as e:
                                error_message = str(e)
                                logging.error(f"Error during remediation: {error_message}")

                            issue = {
                                'type': 'is_person_mismatch',
                                'post_uuid': post_uuid,
                                'post_label': post_label,
                                'membership_id': membership_id,
                                'dataspot_person_uuid': dataspot_person_uuid,
                                'user_email': sk_email,
                                'expected_is_person': expected_is_person,
                                'actual_is_person': actual_is_person,
                                'message': f"User isPerson mismatch: expected '{expected_is_person}', got '{actual_is_person}'",
                                'remediation_attempted': True,
                                'remediation_success': remediation_success,
                                'remediation_steps': remediation_steps,
                                'remediation_error': error_message
                            }
                            check_results['issues'].append(issue)
                        else:
                            logging.info(f"✓ User {sk_email} has correct isPerson field: '{actual_is_person}'")

                    if not user_found:
                        # Try to remediate by creating a user for the email from Staatskalender
                        remediation_success = False
                        remediation_steps = []
                        error_message = None

                        try:
                            logging.info(f"Attempting to create user for email {sk_email}")

                            # Create a new user with EDITOR access level
                            # First ensure person exists
                            person_uuid, person_newly_created = dataspot_client.ensure_person_exists(sk_first_name, sk_last_name)
                            if person_newly_created and not person_uuid:
                                raise ValueError("Failed to create person")
                                
                            # Then ensure user exists linked to that person
                            user_uuid, user_newly_created = dataspot_client.ensure_user_exists(sk_email, person_uuid, access_level="EDITOR")
                            if user_newly_created:
                                remediation_steps.append("created_user")
                                remediation_success = True
                                logging.info(f"Successfully created user for email {sk_email}")
                        except Exception as e:
                            error_message = str(e)
                            logging.error(f"Error during remediation: {error_message}")

                        # Set appropriate message based on remediation success
                        message = f"No user found with email {sk_email} matching the person in Staatskalender"
                        if remediation_success:
                            message = f"Added user with email {sk_email} matching the person in Staatskalender"
                            
                        issue = {
                            'type': 'missing_user',
                            'post_uuid': post_uuid,
                            'post_label': post_label,
                            'membership_id': membership_id,
                            'dataspot_person_uuid': dataspot_person_uuid,
                            'sk_email': sk_email,
                            'message': message,
                            'remediation_attempted': True,
                            'remediation_success': remediation_success,
                            'remediation_steps': remediation_steps,
                            'remediation_error': error_message
                        }
                        check_results['issues'].append(issue)

            # Update check results based on issues found
            if len(check_results['issues']) == 0:
                check_results['status'] = 'success'
                check_results['message'] = "All Data Owner posts have correct person assignments."
            else:
                check_results['status'] = 'warning'
                check_results['message'] = f"Found {len(check_results['issues'])} issues with Data Owner posts."

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
        logging.info("")
        logging.info(f"Data Owner check - Status: {check_results['status']}")
        logging.info(f"Data Owner check - Message: {check_results['message']}")

        if check_results['issues']:
            logging.info(f"Data Owner check - Found {len(check_results['issues'])} issues")

        return check_results
