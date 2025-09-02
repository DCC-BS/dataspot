import logging
import requests
from typing import Any, Dict, List, Optional, Tuple

import config
from src.clients.base_client import BaseDataspotClient
from src.common import requests_get, requests_patch


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


def extract_sk_person_id(person_link: str) -> Optional[str]:
    """
    Extract person ID from Staatskalender person link.
    
    Args:
        person_link: URL to person details in Staatskalender
        
    Returns:
        Person ID or None if extraction failed
    """
    if not person_link:
        return None
        
    # Try to extract the ID from the URL (last part after the last slash)
    try:
        return person_link.strip('/').split('/')[-1]
    except:
        return None


def get_person_details_from_staatskalender(person_link: str) -> Dict[str, Any]:
    """
    Get person details from Staatskalender API.
    
    Args:
        person_link: URL to person details in Staatskalender
        
    Returns:
        Dict containing:
        - 'success': Boolean indicating if the request was successful
        - 'data': Person data dict with first_name, last_name, email, and sk_person_id
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
    sk_person_id = extract_sk_person_id(person_link)
    
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
            'email': email,
            'sk_person_id': sk_person_id
        },
        'status_code': person_response.status_code
    }


def update_person_sk_id(dataspot_client: BaseDataspotClient, person_uuid: str, sk_person_id: str) -> bool:
    """
    Update the sk_person_id custom property of a person in Dataspot.
    
    Args:
        dataspot_client: The Dataspot client
        person_uuid: UUID of the person to update
        sk_person_id: Staatskalender person ID to set
        
    Returns:
        bool: True if successful, False otherwise
    """
    logging.info(f"Updating sk_person_id for person {person_uuid} to {sk_person_id}")
    
    try:
        # Construct the URL for the person update endpoint
        person_url = f"{dataspot_client.base_url}/rest/{dataspot_client.database_name}/persons/{person_uuid}"
        
        # Create minimal payload with _type and customProperties
        payload = {
            "_type": "Person",
            "customProperties": {
                "sk_person_id": sk_person_id
            }
        }
        
        # Send the PATCH request to update the person
        response = requests_patch(
            url=person_url,
            json=payload,
            headers=dataspot_client.auth.get_headers()
        )
        
        if response.status_code not in [200, 201]:
            logging.error(f"Failed to update person sk_person_id. Status code: {response.status_code}")
            return False
            
        logging.info(f"Successfully updated sk_person_id for person {person_uuid}")
        return True
        
    except Exception as e:
        logging.error(f"Failed to update person sk_person_id: {str(e)}")
        return False


def process_post_and_update_person(dataspot_client: BaseDataspotClient, post_uuid: str, post_label: str, membership_id: str, 
                                    persons_by_post: Dict[str, List], post_to_sk_person_id: Dict[str, str]) -> List[Dict[str, Any]]:
    """
    Process a post and update the sk_person_id for associated person.
    
    Args:
        dataspot_client: The Dataspot client
        post_uuid: UUID of the post
        post_label: Label of the post
        membership_id: Membership ID from Staatskalender
        persons_by_post: Mapping of post UUIDs to lists of person details
        post_to_sk_person_id: Output mapping of post UUIDs to SK person IDs
        
    Returns:
        List of issues encountered
    """
    issues = []
    
    # Step 1: Check if membership_id exists
    if not membership_id:
        logging.warning(f"MISSING MEMBERSHIP: {dataspot_client.base_url}/web/{dataspot_client.database_name}/posts/{post_uuid}")
        issues.append({
            'type': 'missing_membership',
            'post_uuid': post_uuid,
            'post_label': post_label,
            'message': f"Post does not have a membership_id"
        })
        return issues
    
    # Step 2: Get membership details from Staatskalender
    membership_result = get_membership_details(membership_id)
    
    if not membership_result['success']:
        membership_url = f"https://staatskalender.bs.ch/api/memberships/{membership_id}"
        logging.warning(f"INVALID MEMBERSHIP: Post '{post_label}' (UUID: {post_uuid}) has invalid membership_id '{membership_id}'")
        issues.append({
            'type': 'invalid_membership',
            'post_uuid': post_uuid,
            'post_label': post_label,
            'membership_id': membership_id,
            'message': f"Membership ID not found in Staatskalender. Status code: {membership_result['status_code']}, invalid url: {membership_url}"
        })
        return issues
    
    membership_data = membership_result['data']
    
    # Step 3: Extract person link from membership data
    person_link = get_person_link_from_membership(membership_data)
    
    if not person_link:
        issues.append({
            'type': 'missing_person_link',
            'post_uuid': post_uuid,
            'post_label': post_label,
            'membership_id': membership_id,
            'message': f"Could not find person link in membership data"
        })
        return issues
    
    # Step 4: Get person details from Staatskalender
    person_result = get_person_details_from_staatskalender(person_link)
    
    if not person_result['success']:
        issues.append({
            'type': 'person_data_error',
            'post_uuid': post_uuid,
            'post_label': post_label,
            'membership_id': membership_id,
            'message': f"Could not retrieve person data from Staatskalender. Status code: {person_result['status_code']}"
        })
        return issues
    
    # Extract person details
    person_data = person_result['data']
    sk_first_name = person_data['first_name']
    sk_last_name = person_data['last_name']
    sk_person_id = person_data['sk_person_id']
    
    if not sk_first_name or not sk_last_name or not sk_person_id:
        issues.append({
            'type': 'missing_person_data',
            'post_uuid': post_uuid,
            'post_label': post_label,
            'membership_id': membership_id,
            'message': f"Could not extract complete person data from Staatskalender"
        })
        return issues
    
    # Add to mapping
    post_to_sk_person_id[post_uuid] = sk_person_id
    
    # Step 5: Find associated person in Dataspot using the cached persons data
    dataspot_persons_info = persons_by_post.get(post_uuid, [])
    
    # Check if no person is assigned to this Data Owner post
    if not dataspot_persons_info:
        issues.append({
            'type': 'no_person_assigned',
            'post_uuid': post_uuid,
            'post_label': post_label,
            'membership_id': membership_id,
            'sk_first_name': sk_first_name,
            'sk_last_name': sk_last_name,
            'sk_person_id': sk_person_id,
            'message': f"No person assigned to this post in Dataspot"
        })
        return issues

    # Process each person assigned to this Data Owner post
    for dataspot_person_info in dataspot_persons_info:
        dataspot_person_uuid = dataspot_person_info.get('uuid')
        dataspot_person_sk_id = dataspot_person_info.get('sk_person_id')

        # Skip if sk_person_id has not changed
        if dataspot_person_sk_id == sk_person_id:
            logging.debug(f"Skipping update of sk_person_id for person {dataspot_person_uuid} (Name: {dataspot_person_info.get('given_name')} {dataspot_person_info.get('family_name')}) because it already has the correct sk_person_id")
            continue

        # Update the person's sk_person_id 
        if update_person_sk_id(dataspot_client, dataspot_person_uuid, sk_person_id):
            # TODO CRITICAL: Add an issue that the sk_person_id has changed, including remediation information (as used elsewhere)!
            logging.info(f"Updated sk_person_id for person {dataspot_person_uuid} (Name: {dataspot_person_info.get('given_name')} {dataspot_person_info.get('family_name')})")
        else:
            issues.append({
                'type': 'sk_id_update_failed',
                'post_uuid': post_uuid,
                'post_label': post_label,
                'person_uuid': dataspot_person_uuid,
                'sk_person_id': sk_person_id,
                'message': f"Failed to update sk_person_id for person {dataspot_person_uuid}"
            })
    
    return issues


def sync_staatskalender_person_ids(dataspot_client: BaseDataspotClient, data_owner_posts: List[Dict[str, Any]]) -> Tuple[List[Dict[str, Any]], Dict[str, str]]:
    """
    Sync Staatskalender person IDs to Dataspot persons.
    
    Args:
        dataspot_client: The Dataspot client
        data_owner_posts: List of Data Owner posts
        
    Returns:
        Tuple containing:
        - List of issues encountered
        - Mapping of post UUIDs to SK person IDs
    """
    issues = []
    post_to_sk_person_id = {}
    
    # Get all post details with membership_ids
    post_count = len(data_owner_posts)
    logging.info(f"Syncing Staatskalender person IDs for {post_count} Data Owner posts")
    
    # Build person mapping at the beginning
    persons_by_post = build_persons_by_post_mapping(dataspot_client)
    
    # Process each post
    for idx, post in enumerate(data_owner_posts):
        post_uuid = post['uuid']
        post_label = post['post_label']
        
        logging.info(f"[{idx + 1}/{post_count}] Processing post: {post_label}")
        
        # Get post details to extract membership_id
        post_url = f"{config.base_url}/api/{config.database_name}/posts/{post_uuid}"
        post_response = requests_get(
            url=post_url,
            headers=dataspot_client.auth.get_headers()
        )
        
        if post_response.status_code != 200:
            logging.warning(f"Could not retrieve post data for '{post_label}' (UUID: {post_uuid}). Status code: {post_response.status_code}")
            issues.append({
                'type': 'error',
                'post_uuid': post_uuid,
                'post_label': post_label,
                'message': f"Could not retrieve post data from Dataspot. Status code: {post_response.status_code}"
            })
            continue
        
        post_data = post_response.json().get('asset', {})
        membership_id = post_data.get('customProperties', {}).get('membership_id')
        
        # Process post and update associated person
        post_issues = process_post_and_update_person(
            dataspot_client, 
            post_uuid, 
            post_label, 
            membership_id, 
            persons_by_post,
            post_to_sk_person_id
        )
        
        issues.extend(post_issues)
    
    logging.info(f"Completed syncing Staatskalender person IDs with {len(issues)} issues found")
    return issues, post_to_sk_person_id


def build_persons_by_post_mapping(dataspot_client: BaseDataspotClient) -> Dict[str, List[Dict[str, Any]]]:
    """
    Builds a mapping from post UUIDs to lists of person details.

    This function uses SQL query API to fetch all persons and their posts from Dataspot 
    and creates a dictionary where keys are post UUIDs and values are lists of person details.
    Each post can have multiple persons assigned to it, and each person can be assigned to multiple posts.

    Args:
        dataspot_client: The client for connecting to Dataspot API

    Returns:
        Dict[str, List[Dict[str, Any]]]: Mapping of post UUIDs to lists of person details
    """
    persons_by_post = {}

    # SQL query to fetch all persons and their associated posts
    sql_query = """
    SELECT 
      p.id AS person_uuid,
      p.given_name,
      p.family_name,
      hp.holds_post AS post_uuid,
      cp.value AS sk_person_id
    FROM 
      person_view p
    JOIN
      holdspost_view hp ON p.id = hp.resource_id
    LEFT JOIN
      customproperties_view cp ON p.id = cp.resource_id AND cp.name = 'sk_person_id'
    """

    # Execute query via Dataspot Query API
    logging.info("Executing query to fetch persons and their posts...")
    result = dataspot_client.execute_query_api(sql_query=sql_query)

    # Process the query results to build the mapping
    for row in result:
        person_uuid = row.get('person_uuid')
        given_name = row.get('given_name')
        family_name = row.get('family_name')
        post_uuid = row.get('post_uuid')
        sk_person_id = row.get('sk_person_id')
        if sk_person_id:
            sk_person_id = sk_person_id.strip('"')

        if post_uuid not in persons_by_post:
            persons_by_post[post_uuid] = []

        persons_by_post[post_uuid].append({
            'uuid': person_uuid,
            'given_name': given_name,
            'family_name': family_name,
            'sk_person_id': sk_person_id
        })

    logging.info(f"Built a mapping of {len(persons_by_post)} posts to their associated persons")
    return persons_by_post
