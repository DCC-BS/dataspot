import logging
import traceback
from typing import Any, Dict, List

import config
from src.clients.base_client import BaseDataspotClient
from src.common import requests_get, requests_patch
import requests

def update_person_holdspost(dataspot_client: BaseDataspotClient, person_uuid: str, post_uuid: str, add: bool = True) -> (bool, bool):
    """
    Updates a person's holdsPost array by either adding or removing a data owner post.

    Args:
        dataspot_client: The Dataspot client
        person_uuid: UUID of the person
        post_uuid: UUID of the Data Owner post
        add: True to add post, False to remove post

    Returns:
        (bool, bool): (success, has_remaining_posts)
            - success: True if operation was successful, False otherwise
            - has_remaining_posts: True if person still has posts after operation, False if they have no posts
    """
    if add:
        logging.debug(f"Attempting to add Data Owner post {post_uuid} to person {person_uuid}")
    else:
        logging.debug(f"Attempting to remove Data Owner post {post_uuid} from person {person_uuid}")

    try:
        # Get the current person data
        person_url = f"{dataspot_client.base_url}/rest/{dataspot_client.database_name}/persons/{person_uuid}"
        response = requests_get(
            url=person_url,
            headers=dataspot_client.auth.get_headers()
        )

        if response.status_code != 200:
            logging.error(f"Failed to retrieve person with UUID {person_uuid}. Status code: {response.status_code}")
            return False, False

        person_data = response.json()

        # Get current posts
        current_posts = person_data.get('holdsPost', [])

        # Add or remove the post
        if add and post_uuid not in current_posts:
            current_posts.append(post_uuid)
        elif not add and post_uuid in current_posts:
            current_posts.remove(post_uuid)
        else:
            # No change needed
            logging.info(f"No change needed. Post {post_uuid} is {'already' if add else 'not'} in person's holdsPost array.")
            has_remaining_posts = len(current_posts) > 0
            return True, has_remaining_posts

        # Create minimal json data object with updated posts
        person_update_json = {
            '_type': 'Person',
            'holdsPost': current_posts
        }

        # Send update request
        response = requests_patch(
            url=person_url,
            json=person_update_json,
            headers=dataspot_client.auth.get_headers()
        )

        if response.status_code not in [200, 201]:
            logging.error(f"Failed to update person. Status code: {response.status_code}")
            return False, False

        has_remaining_posts = len(current_posts) > 0
        
        if add:
            logging.info(f"Successfully added Data Owner post {post_uuid} to person {person_uuid}")
        else:
            logging.info(f"Successfully removed Data Owner post {post_uuid} from person {person_uuid}")
            if not has_remaining_posts:
                logging.info(f"Person {person_uuid} no longer has any posts")
                
        return True, has_remaining_posts

    except Exception as e:
        action = "add" if add else "remove"
        logging.error(f"Failed to {action} Data Owner post for person: {str(e)}")
        return False, False

def set_user_access_level(dataspot_client: BaseDataspotClient, user_id: str, access_level: str) -> bool:
    """
    Set the access level of a user.
    
    Args:
        dataspot_client: The Dataspot client
        user_id: UUID of the user to set the access level for
        access_level: The access level to set the user to (e.g., "READ_ONLY", "EDITOR", "ADMINISTRATOR")
        
    Returns:
        bool: True if successful, False otherwise
    """
    logging.info(f"Attempting to set access level for user {user_id} to {access_level}")
    try:
        # Create minimal payload with _type and accessLevel
        payload = {
            "_type": "User",
            "accessLevel": access_level
        }
        
        # Construct the URL for the user update endpoint
        update_url = f"{dataspot_client.base_url}/rest/{dataspot_client.database_name}/users/{user_id}"
        
        # Send the PATCH request to update the user
        response = requests_patch(
            url=update_url,
            json=payload,
            headers=dataspot_client.auth.get_headers()
        )
        
        # Raise an exception if the request fails
        response.raise_for_status()
        
        logging.info(f"Successfully updated access level for user {user_id} to {access_level}")
        return True
    except Exception as e:
        logging.error(f"Failed to set user access level: {str(e)}")
        return False

def update_user_is_person(dataspot_client: BaseDataspotClient, user_email: str, person_name: str) -> bool:
    """
    Update the isPerson field of a user.
    
    Args:
        dataspot_client: The Dataspot client
        user_email: Email of the user to update
        person_name: Name in format "Last name, First name" for isPerson field
        
    Returns:
        bool: True if successful, False otherwise
    """
    logging.info(f"Attempting to update isPerson field for user {user_email} to '{person_name}'")
    
    try:
        # Step 1: Find the user by email using REST API
        users_url = f"{dataspot_client.base_url}/api/{dataspot_client.database_name}/tenants/Mandant/download?format=JSON"
        users_response = requests_get(
            url=users_url,
            headers=dataspot_client.auth.get_headers()
        )
        
        if users_response.status_code != 200:
            logging.error(f"Failed to retrieve users. Status code: {users_response.status_code}")
            return False
        
        users_data = users_response.json()
        user_id = None
        
        # Find the user with the matching email
        for user in users_data:
            if user.get('loginId') == user_email:
                user_id = user.get('id')
                break
        
        if not user_id:
            logging.error(f"User with email {user_email} not found")
            return False
            
        # Step 2: Update the user's isPerson field
        update_url = f"{dataspot_client.base_url}/rest/{dataspot_client.database_name}/users/{user_id}"
        
        # Create minimal payload with _type and isPerson
        payload = {
            "_type": "User",
            "isPerson": person_name
        }
        
        # Send the PATCH request to update the user
        response = requests_patch(
            url=update_url,
            json=payload,
            headers=dataspot_client.auth.get_headers()
        )
        
        if response.status_code not in [200, 201]:
            logging.error(f"Failed to update user isPerson field. Status code: {response.status_code}")
            return False
        
        logging.info(f"Successfully updated isPerson field for user {user_email} to '{person_name}'")
        return True
        
    except Exception as e:
        logging.error(f"Failed to update user isPerson field: {str(e)}")
        return False

def build_users_by_email_mapping(dataspot_client: BaseDataspotClient) -> Dict[str, Dict[str, Any]]:
    """
    Builds a mapping from user email addresses to user details.
    
    This function fetches all users from Dataspot and creates a dictionary
    where keys are email addresses (loginId) and values are user details.
    
    Returns:
        Dict[str, Dict[str, Any]]: Mapping of email addresses to user details
    """
    users_by_email = {}

    # Fetch users from dataspot
    users_url = f"{dataspot_client.base_url}/api/{dataspot_client.database_name}/tenants/{config.tenant_name}/download?format=JSON"
    users_response = requests_get(
        url=users_url,
        headers=dataspot_client.auth.get_headers()
    )

    if users_response.status_code != 200:
        logging.error(f"Could not fetch users data from Dataspot. Status code: {users_response.status_code}")
        return users_by_email

    users_data = users_response.json()

    # Process each user to build the mapping
    for user in users_data:
        email = user.get('loginId')
        if email:
            users_by_email[email] = user

    logging.info(f"Built a mapping of {len(users_by_email)} email addresses to their associated users")
    return users_by_email

def build_persons_by_post_mapping(dataspot_client: BaseDataspotClient) -> Dict[str, List[Dict[str, Any]]]:
    """
    Builds a mapping from post UUIDs to lists of person details.
    
    This function fetches all persons from Dataspot and creates a dictionary
    where keys are post UUIDs and values are lists of person details.
    Each post can have multiple persons assigned to it.
    
    Returns:
        Dict[str, List[Dict[str, Any]]]: Mapping of post UUIDs to lists of person details
    """
    persons_by_post = {}

    # Fetch all persons from Dataspot
    persons_url = f"{dataspot_client.base_url}/rest/{dataspot_client.database_name}/persons"
    persons_response = requests_get(
        url=persons_url,
        headers=dataspot_client.auth.get_headers()
    )

    if persons_response.status_code != 200:
        logging.error(f"Could not fetch persons data from Dataspot. Status code: {persons_response.status_code}")
        return persons_by_post

    persons_data = persons_response.json()

    # Process each person to build the mapping
    for person in persons_data.get('_embedded', {}).get('persons', []):
        person_uuid = person.get('id')
        given_name = person.get('givenName')
        family_name = person.get('familyName')

        # Get the posts this person holds
        holds_posts = person.get('holdsPost', [])

        # Add mapping for each post
        for post_uuid in holds_posts:
            if post_uuid not in persons_by_post:
                persons_by_post[post_uuid] = []

            persons_by_post[post_uuid].append({
                'uuid': person_uuid,
                'given_name': given_name,
                'family_name': family_name,
            })

    logging.info(f"Built a mapping of {len(persons_by_post)} posts to their associated persons")
    return persons_by_post


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

        # Process results
        if isinstance(result, list):
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
                post_url = f"{config.base_url}/api/{config.database_name}/posts/{post_uuid}"

                post_response = requests_get(
                    url=post_url,
                    headers=dataspot_client.auth.get_headers()
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
                    continue

                post_data = post_response.json().get('asset', {})

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
                membership_url = f"https://staatskalender.bs.ch/api/memberships/{membership_id}"
                logging.info(f"Checking membership_id {membership_id} in Staatskalender...")
                logging.info(f"Retrieving membership data from Staatskalender: {membership_url}")

                membership_response = requests.get(url=membership_url)

                if membership_response.status_code != 200:
                    # Log the invalid membership ID immediately
                    logging.warning \
                        (f"INVALID MEMBERSHIP: Post '{post_label}' (UUID: {post_uuid}) has invalid membership_id '{membership_id}'. Status code: {membership_response.status_code}")

                    issue = {
                        'type': 'invalid_membership',
                        'post_uuid': post_uuid,
                        'post_label': post_label,
                        'membership_id': membership_id,
                        'message': f"Membership ID not found in Staatskalender. Invalid URL: {membership_url}"
                    }
                    check_results['issues'].append(issue)
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
                        continue

                    # Step 4: Get person data from Staatskalender
                    logging.info(f"Retrieving person data from Staatskalender: {person_link}")
                    person_staatskalender_response = requests_get(url=person_link, headers={}) # We don't need dataspot headers here

                    if person_staatskalender_response.status_code != 200:
                        issue = {
                            'type': 'person_data_error',
                            'post_uuid': post_uuid,
                            'post_label': post_label,
                            'membership_id': membership_id,
                            'message': f"Could not retrieve person data from Staatskalender. Status code: {person_staatskalender_response.status_code}"
                        }
                        check_results['issues'].append(issue)
                        continue

                    # Extract first and last name from Staatskalender person
                    person_staatskalender_data = person_staatskalender_response.json()
                    sk_first_name = None
                    sk_last_name = None
                    sk_email = None

                    for item in person_staatskalender_data.get('collection', {}).get('items', []):
                        for data_item in item.get('data', []):
                            if data_item.get('name') == 'first_name':
                                sk_first_name = data_item.get('value')
                            elif data_item.get('name') == 'last_name':
                                sk_last_name = data_item.get('value')
                            elif data_item.get('name') == 'email':
                                sk_email = data_item.get('value').lower()

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
                        # Try to remediate the person mismatch if we have email from Staatskalender
                        if sk_email:
                            # Get the current user email for downgrading access, if available
                            current_user_email = None
                            for email, user in users_by_email.items():
                                # Look for a user with isPerson matching the dataspot person
                                expected_is_person = f"{dataspot_last_name}, {dataspot_first_name}"
                                if user.get('isPerson') == expected_is_person:
                                    current_user_email = email
                                    break

                            remediation_success = False
                            remediation_steps = []
                            error_message = None

                            try:
                                logging.info(f"Attempting to remediate person mismatch for post {post_label}")

                                # 1. Remove the Data Owner Post from the current person if one exists
                                if dataspot_person_uuid:
                                    success, has_remaining_posts = update_person_holdspost(dataspot_client, dataspot_person_uuid, post_uuid, add=False)
                                    if success:
                                        remediation_steps.append("removed_data_owner_post")
                                        
                                        # 2. If the person now doesn't hold any posts, set access level to READ_ONLY
                                        if current_user_email and not has_remaining_posts:
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
                                            user_check = dataspot_client.get_user_by_email(sk_email)
                                            if not user_check or user_check.get('accessLevel') != "EDITOR":
                                                logging.error(f"User creation succeeded but properties incorrect")
                                            remediation_steps.append("created_new_user")

                                    remediation_success = True
                                    logging.info(f"Successfully remediated Data Owner post {post_label}")
                                    
                                    # After successful remediation, skip to the next post
                                    # instead of adding this as an issue
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
                            except Exception as e:
                                error_message = str(e)
                                logging.error(f"Error during remediation: {error_message}")

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
                                'message': f"Person mismatch: Staatskalender ({sk_first_name} {sk_last_name}) vs. Dataspot ({dataspot_first_name} {dataspot_last_name})",
                                'remediation_attempted': True,
                                'remediation_success': remediation_success,
                                'remediation_steps': remediation_steps,
                                'remediation_error': error_message
                            }
                        else:
                            # No remediation possible without email
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
                                'message': f"Person mismatch: Staatskalender ({sk_first_name} {sk_last_name}) vs. Dataspot ({dataspot_first_name} {dataspot_last_name})",
                                'remediation_attempted': False,
                                'remediation_reason': "No email available from Staatskalender"
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

                            issue = {
                                'type': 'missing_user',
                                'post_uuid': post_uuid,
                                'post_label': post_label,
                                'membership_id': membership_id,
                                'dataspot_person_uuid': dataspot_person_uuid,
                                'sk_email': sk_email,
                                'message': f"No user found with email {sk_email} matching the person in Staatskalender",
                                'remediation_attempted': True,
                                'remediation_success': remediation_success,
                                'remediation_steps': remediation_steps,
                                'remediation_error': error_message
                            }
                            check_results['issues'].append(issue)

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
                    logging.error(f"Error processing post {post_label}: {str(e)}")

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