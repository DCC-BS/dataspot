import logging
from typing import Any, Dict, List

import config
from src.clients.base_client import BaseDataspotClient
from src.common import requests_get, requests_patch


def update_person_holdspost(dataspot_client: BaseDataspotClient, person_uuid: str, post_uuid: str,
                            add: bool = True) -> (bool, bool):
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
            logging.info(
                f"No change needed. Post {post_uuid} is {'already' if add else 'not'} in person's holdsPost array.")
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


def get_user_by_email(dataspot_client: BaseDataspotClient, email: str) -> Dict[str, Any] | None:
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
