import logging
from typing import Any, Dict, List, Optional, Tuple

import config
from src.clients.base_client import BaseDataspotClient
from src.common import requests_get, requests_patch


def get_person_by_sk_id(dataspot_client: BaseDataspotClient, sk_person_id: str) -> Dict[str, Any]:
    """
    Find a person in Dataspot by sk_person_id.
    
    Args:
        dataspot_client: The Dataspot client
        sk_person_id: Staatskalender person ID
        
    Returns:
        Dict containing person details or empty dict if not found
    """
    # Fetch all persons from Dataspot
    persons_url = f"{dataspot_client.base_url}/rest/{dataspot_client.database_name}/persons"
    persons_response = requests_get(
        url=persons_url,
        headers=dataspot_client.auth.get_headers()
    )

    if persons_response.status_code != 200:
        logging.error(f"Could not fetch persons data from Dataspot. Status code: {persons_response.status_code}")
        return {}

    persons_data = persons_response.json()
    
    # Find person with matching sk_person_id
    for person in persons_data.get('_embedded', {}).get('persons', []):
        person_custom_props = person.get('customProperties', {})
        if person_custom_props.get('sk_person_id') == sk_person_id:
            return person
    
    return {}


def get_sk_person_details(dataspot_client: BaseDataspotClient, sk_person_id: str) -> Dict[str, Any]:
    """
    Get person details from Staatskalender using sk_person_id.
    
    Args:
        dataspot_client: The Dataspot client (for headers)
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
      hp.holds_post AS post_uuid
    FROM
      person_view p
    JOIN
      holdspost_view hp ON p.id = hp.resource_id
    """
    
    logging.info("Executing query to get post assignments...")
    posts_result = dataspot_client.execute_query_api(sql_query=posts_query)
    
    # Build a mapping of person_id to posts
    person_posts = {}
    for row in posts_result:
        person_id = row.get('person_id')
        post_uuid = row.get('post_uuid')
        if person_id not in person_posts:
            person_posts[person_id] = []
        if post_uuid:
            person_posts[person_id].append(post_uuid)
    
    # Process results to match the expected structure
    persons_with_sk_id = []
    for row in result:
        person_id = row.get('id')
        # Get this person's posts from our mapping
        holds_post = person_posts.get(person_id, [])
        
        # Format each person to match the structure expected by the rest of the code
        person = {
            'id': person_id,
            'givenName': row.get('givenName'),
            'familyName': row.get('familyName'),
            'customProperties': {
                'sk_person_id': row.get('sk_person_id').strip('"') if row.get('sk_person_id') else None
            },
            'holdsPost': holds_post
        }
        persons_with_sk_id.append(person)
    
    logging.info(f"Found {len(persons_with_sk_id)} persons with sk_person_id")
    return persons_with_sk_id


def ensure_user_exists(dataspot_client: BaseDataspotClient, email: str, person_uuid: str, access_level: str = "EDITOR") -> Tuple[str, bool]:
    """
    Ensure a user exists with the given email and is linked to the person.
    
    Args:
        dataspot_client: The Dataspot client
        email: Email address for the user
        person_uuid: UUID of the person to link the user to
        access_level: Access level to set for the user (default: EDITOR)
        
    Returns:
        Tuple of (user_uuid, newly_created)
    """
    # Check if user already exists
    user = get_user_by_email(dataspot_client, email)
    
    if user:
        # User exists, ensure it has the right access level
        user_id = user.get('id')
        current_access_level = user.get('accessLevel')
        
        if current_access_level != access_level:
            set_user_access_level(dataspot_client, user_id, access_level)
        
        return user_id, False
    
    # User doesn't exist, create it
    logging.info(f"Creating new user with email {email}")
    
    # Get person details for isPerson field
    person_url = f"{dataspot_client.base_url}/rest/{dataspot_client.database_name}/persons/{person_uuid}"
    person_response = requests_get(
        url=person_url,
        headers=dataspot_client.auth.get_headers()
    )
    
    if person_response.status_code != 200:
        logging.error(f"Could not retrieve person data. Status code: {person_response.status_code}")
        return "", False
    
    person_data = person_response.json()
    given_name = person_data.get('givenName')
    family_name = person_data.get('familyName')
    is_person_value = f"{family_name}, {given_name}"
    
    # Create user
    user_data = {
        "_type": "User",
        "loginId": email,
        "name": f"{given_name} {family_name}",
        "isPerson": is_person_value,
        "accessLevel": access_level
    }
    
    users_url = f"{dataspot_client.base_url}/rest/{dataspot_client.database_name}/users"
    response = requests_patch(
        url=users_url,
        json=user_data,
        headers=dataspot_client.auth.get_headers()
    )
    
    if response.status_code not in [200, 201]:
        logging.error(f"Failed to create user. Status code: {response.status_code}")
        return "", False
    
    # Get the created user ID
    new_user = get_user_by_email(dataspot_client, email)
    if new_user:
        return new_user.get('id'), True
    
    return "", False


def sync_users_for_persons_with_sk_id(dataspot_client: BaseDataspotClient) -> List[Dict[str, Any]]:
    """
    Sync users for all persons with sk_person_id.
    
    Args:
        dataspot_client: The Dataspot client
        
    Returns:
        List of issues encountered
    """
    issues = []
    
    # Get all persons with sk_person_id
    persons = get_all_persons_with_sk_id(dataspot_client)
    
    logging.info(f"Processing {len(persons)} persons with sk_person_id")
    
    # Process each person
    for idx, person in enumerate(persons):
        person_uuid = person.get('id')
        given_name = person.get('givenName')
        family_name = person.get('familyName')
        sk_person_id = person.get('customProperties', {}).get('sk_person_id')
        
        logging.info(f"[{idx + 1}/{len(persons)}] Processing person: {given_name} {family_name} (sk_id: {sk_person_id})")
        
        # Get Staatskalender details for this person
        sk_person_details = get_sk_person_details(dataspot_client, sk_person_id)
        
        if not sk_person_details:
            issues.append({
                'type': 'sk_person_not_found',
                'person_uuid': person_uuid,
                'given_name': given_name,
                'family_name': family_name,
                'sk_person_id': sk_person_id,
                'message': f"Could not find person with ID {sk_person_id} in Staatskalender"
            })
            continue
        
        # Check if email is available
        email = sk_person_details.get('email')
        if not email:
            issues.append({
                'type': 'missing_email',
                'person_uuid': person_uuid,
                'given_name': given_name,
                'family_name': family_name,
                'sk_person_id': sk_person_id,
                'message': f"Person {given_name} {family_name} has no email in Staatskalender"
            })
            continue
        
        # Check if this person has Data Owner posts
        holds_posts = person.get('holdsPost', [])
        has_data_owner_role = False
        
        if holds_posts:
            # Check if any of these posts have the Data Owner role
            for post_uuid in holds_posts:
                post_url = f"{config.base_url}/api/{config.database_name}/posts/{post_uuid}"
                post_response = requests_get(
                    url=post_url,
                    headers=dataspot_client.auth.get_headers()
                )
                
                if post_response.status_code == 200:
                    post_data = post_response.json().get('asset', {})
                    post_roles = post_data.get('hasRole', [])
                    
                    # Find the Data Owner role
                    for role_uuid in post_roles:
                        role_url = f"{config.base_url}/api/{config.database_name}/roles/{role_uuid}"
                        role_response = requests_get(
                            url=role_url,
                            headers=dataspot_client.auth.get_headers()
                        )
                        
                        if role_response.status_code == 200:
                            role_data = role_response.json().get('asset', {})
                            if role_data.get('label') == 'Data Owner':
                                has_data_owner_role = True
                                break
                    
                    if has_data_owner_role:
                        break
        
        # Determine appropriate access level
        access_level = "EDITOR" if has_data_owner_role else "READ_ONLY"
        
        # Ensure user exists with proper access level
        user_uuid, newly_created = ensure_user_exists(dataspot_client, email, person_uuid, access_level)
        
        if not user_uuid:
            issues.append({
                'type': 'user_creation_failed',
                'person_uuid': person_uuid,
                'given_name': given_name,
                'family_name': family_name,
                'sk_person_id': sk_person_id,
                'email': email,
                'message': f"Failed to create/update user for {email}"
            })
            continue
        
        # User was created/updated successfully
        action = "Created new" if newly_created else "Updated existing"
        logging.info(f"{action} user for {given_name} {family_name} with email {email} and access level {access_level}")
    
    logging.info(f"Completed user sync with {len(issues)} issues found")
    return issues


def ensure_correct_post_assignments(dataspot_client: BaseDataspotClient, data_owner_posts: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    Ensure all posts are assigned to the correct person based on sk_person_id.
    
    Args:
        dataspot_client: The Dataspot client
        data_owner_posts: List of Data Owner posts
        
    Returns:
        List of issues encountered
    """
    issues = []
    
    post_count = len(data_owner_posts)
    logging.info(f"Verifying correct post assignments for {post_count} Data Owner posts")
    
    # Process each post
    for idx, post in enumerate(data_owner_posts):
        post_uuid = post.get('uuid')
        post_label = post.get('post_label', 'Unknown')
        
        logging.info(f"[{idx + 1}/{post_count}] Verifying post: {post_label}")
        
        # Get post details to extract membership_id
        post_url = f"{config.base_url}/api/{config.database_name}/posts/{post_uuid}"
        post_response = requests_get(
            url=post_url,
            headers=dataspot_client.auth.get_headers()
        )
        
        if post_response.status_code != 200:
            issues.append({
                'type': 'error',
                'post_uuid': post_uuid,
                'post_label': post_label,
                'message': f"Could not retrieve post data from Dataspot. Status code: {post_response.status_code}"
            })
            continue
        
        post_data = post_response.json().get('asset', {})
        membership_id = post_data.get('customProperties', {}).get('membership_id')
        
        # Skip posts without membership_id - we can't verify these
        if not membership_id:
            continue
        
        # Get the current person assignments for this post
        post_assignments_url = f"{dataspot_client.base_url}/rest/{dataspot_client.database_name}/posts/{post_uuid}/inverse/holdsPost"
        assignments_response = requests_get(
            url=post_assignments_url,
            headers=dataspot_client.auth.get_headers()
        )
        
        if assignments_response.status_code != 200:
            issues.append({
                'type': 'error',
                'post_uuid': post_uuid,
                'post_label': post_label,
                'message': f"Could not retrieve post assignments. Status code: {assignments_response.status_code}"
            })
            continue
        
        assignments_data = assignments_response.json()
        assigned_persons = assignments_data.get('_embedded', {}).get('persons', [])
        
        # Skip if no persons are assigned
        if not assigned_persons:
            continue
        
        # If multiple persons are assigned, we'll need to determine the correct one
        if len(assigned_persons) > 1:
            issues.append({
                'type': 'multiple_persons_assigned',
                'post_uuid': post_uuid,
                'post_label': post_label,
                'message': f"Multiple persons assigned to this post, manual intervention required"
            })
            continue
        
        # Get the assigned person
        assigned_person = assigned_persons[0]
        person_uuid = assigned_person.get('id')
        person_custom_props = assigned_person.get('customProperties', {})
        person_sk_id = person_custom_props.get('sk_person_id')
        
        # Skip if person doesn't have sk_person_id
        if not person_sk_id:
            continue
        
        # TODO: Verify that the assigned person is correct based on membership_id
        # This would require fetching Staatskalender data again, which might be redundant
        # since we already set the sk_person_id based on the membership_id in Phase 1
        
    logging.info(f"Completed post assignment verification with {len(issues)} issues found")
    return issues


def get_user_by_email(dataspot_client: BaseDataspotClient, email: str) -> Dict[str, Any]:
    """
    Get user details by email address.
    
    Args:
        dataspot_client: Dataspot client, only used for retrieving headers
        email: Email address of the user to find
        
    Returns:
        Dict[str, Any]: User details if found, None otherwise
    """
    users_url = f"{config.base_url}/api/{config.database_name}/tenants/{config.tenant_name}/download?format=JSON"
    response = requests_get(
        url=users_url,
        headers=dataspot_client.auth.get_headers()
    )
    
    if response.status_code != 200:
        logging.error(f"Could not fetch users data from Dataspot. Status code: {response.status_code}")
        return None
    
    users = response.json()
    
    # Find user with matching email
    for user in users:
        if user.get('loginId') == email:
            return user
    
    return None


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
    logging.info(f"Setting access level for user {user_id} to {access_level}")
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


def update_person_holdspost(dataspot_client: BaseDataspotClient, person_uuid: str, post_uuid: str,
                           add: bool = True) -> Tuple[bool, bool]:
    """
    Updates a person's holdsPost array by either adding or removing a data owner post.
    
    Args:
        dataspot_client: The Dataspot client
        person_uuid: UUID of the person
        post_uuid: UUID of the Data Owner post
        add: True to add post, False to remove post
        
    Returns:
        Tuple of (success, has_remaining_posts)
    """
    if add:
        logging.debug(f"Adding post {post_uuid} to person {person_uuid}")
    else:
        logging.debug(f"Removing post {post_uuid} from person {person_uuid}")
    
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
            logging.info(f"Successfully added post {post_uuid} to person {person_uuid}")
        else:
            logging.info(f"Successfully removed post {post_uuid} from person {person_uuid}")
            if not has_remaining_posts:
                logging.info(f"Person {person_uuid} no longer has any posts")
        
        return True, has_remaining_posts
        
    except Exception as e:
        action = "add" if add else "remove"
        logging.error(f"Failed to {action} post for person: {str(e)}")
        return False, False
