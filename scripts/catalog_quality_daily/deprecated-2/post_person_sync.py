import logging
import traceback
from typing import Any, Dict, List, Optional, Tuple

import config
from src.clients.base_client import BaseDataspotClient
from src.common import requests_get, requests_patch, requests_post


def sync_posts_with_staatskalender_persons(dataspot_client: BaseDataspotClient, data_owner_posts: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    Comprehensive function to sync Data Owner posts with Staatskalender persons.
    
    For each post, this function:
    1. Retrieves all membership IDs (primary and secondary)
    2. Gets person details from Staatskalender
    3. Creates/updates persons in Dataspot with correct SK IDs and names
    4. Ensures posts are linked to the correct persons
    
    Args:
        dataspot_client: The Dataspot client
        data_owner_posts: List of Data Owner posts
        
    Returns:
        List of issues encountered during the process
    """
    issues = []
    post_count = len(data_owner_posts)
    logging.info(f"Processing {post_count} Data Owner posts")
    
    # Process each post
    for idx, post in enumerate(data_owner_posts):
        post_uuid = post['uuid']
        post_label = post['post_label']
        
        logging.info(f"[{idx + 1}/{post_count}] Processing post: {post_label}")
        
        try:
            # Step 1: Get post details including all membership IDs
            post_details = get_post_details(dataspot_client, post_uuid, post_label)
            if not post_details:
                issues.append({
                    'type': 'error',
                    'post_uuid': post_uuid,
                    'post_label': post_label,
                    'message': f"Could not retrieve post details"
                })
                continue
            
            primary_membership_id = post_details.get('membership_id')
            secondary_membership_id = post_details.get('second_membership_id')
            
            if not primary_membership_id and not secondary_membership_id:
                issues.append({
                    'type': 'missing_membership',
                    'post_uuid': post_uuid,
                    'post_label': post_label,
                    'message': f"Post does not have any membership_id"
                })
                continue
            
            # Step 2: Process primary membership if it exists
            if primary_membership_id:
                primary_issues = process_membership_for_post(
                    dataspot_client, 
                    post_uuid, 
                    post_label, 
                    primary_membership_id,
                    is_primary=True
                )
                issues.extend(primary_issues)
            
            # Step 3: Process secondary membership if it exists
            if secondary_membership_id and secondary_membership_id != primary_membership_id:
                secondary_issues = process_membership_for_post(
                    dataspot_client, 
                    post_uuid, 
                    post_label, 
                    secondary_membership_id,
                    is_primary=False
                )
                issues.extend(secondary_issues)
            
        except Exception as e:
            logging.error(f"Error processing post {post_label}: {str(e)}")
            issues.append({
                'type': 'processing_error',
                'post_uuid': post_uuid,
                'post_label': post_label,
                'message': f"Error: {str(e)}"
            })
    
    logging.info(f"Completed processing {post_count} posts with {len(issues)} issues")
    return issues


def get_post_details(dataspot_client: BaseDataspotClient, post_uuid: str, post_label: str) -> Optional[Dict[str, Any]]:
    """
    Get detailed post information including all membership IDs.
    
    Args:
        dataspot_client: The Dataspot client
        post_uuid: UUID of the post
        post_label: Label of the post (for logging)
        
    Returns:
        Dict with post details or None if retrieval failed
    """
    post_url = f"{dataspot_client.base_url}/api/{dataspot_client.database_name}/posts/{post_uuid}"
    
    post_response = requests_get(
        url=post_url,
        headers=dataspot_client.auth.get_headers()
    )
    
    if post_response.status_code != 200:
        logging.warning(f"Could not retrieve post data for '{post_label}' (UUID: {post_uuid}). Status code: {post_response.status_code}")
        return None
    
    post_data = post_response.json().get('asset', {})
    custom_props = post_data.get('customProperties', {})
    
    return {
        'uuid': post_uuid,
        'label': post_label,
        'membership_id': custom_props.get('membership_id'),
        'second_membership_id': custom_props.get('second_membership_id')
    }


def process_membership_for_post(
    dataspot_client: BaseDataspotClient, 
    post_uuid: str, 
    post_label: str, 
    membership_id: str,
    is_primary: bool = True
) -> List[Dict[str, Any]]:
    """
    Process a single membership ID for a post.
    
    Args:
        dataspot_client: The Dataspot client
        post_uuid: UUID of the post
        post_label: Label of the post
        membership_id: Membership ID to process
        is_primary: Whether this is the primary membership for the post
        
    Returns:
        List of issues encountered
    """
    issues = []
    membership_type = "primary" if is_primary else "secondary"
    
    # Step 1: Get membership details from Staatskalender
    membership_result = get_membership_details(membership_id)
    
    if not membership_result['success']:
        membership_url = f"https://staatskalender.bs.ch/api/memberships/{membership_id}"
        logging.warning(f"INVALID MEMBERSHIP: Post '{post_label}' has invalid {membership_type} membership_id '{membership_id}'")
        issues.append({
            'type': 'invalid_membership',
            'post_uuid': post_uuid,
            'post_label': post_label,
            'membership_id': membership_id,
            'membership_type': membership_type,
            'message': f"{membership_type.capitalize()} membership ID not found in Staatskalender. Status code: {membership_result['status_code']}"
        })
        return issues
    
    membership_data = membership_result['data']
    
    # Step 2: Extract person link from membership data
    person_link = get_person_link_from_membership(membership_data)
    
    if not person_link:
        issues.append({
            'type': 'missing_person_link',
            'post_uuid': post_uuid,
            'post_label': post_label,
            'membership_id': membership_id,
            'membership_type': membership_type,
            'message': f"Could not find person link in {membership_type} membership data"
        })
        return issues
    
    # Step 3: Get person details from Staatskalender
    person_result = get_person_details_from_staatskalender(person_link)
    
    if not person_result['success']:
        issues.append({
            'type': 'person_data_error',
            'post_uuid': post_uuid,
            'post_label': post_label,
            'membership_id': membership_id,
            'membership_type': membership_type,
            'message': f"Could not retrieve person data from Staatskalender. Status code: {person_result['status_code']}"
        })
        return issues
    
    # Extract person details
    person_data = person_result['data']
    sk_first_name = person_data['first_name']
    sk_last_name = person_data['last_name']
    sk_person_id = person_data['sk_person_id']
    sk_email = person_data['email']
    
    if not sk_first_name or not sk_last_name or not sk_person_id:
        issues.append({
            'type': 'missing_person_data',
            'post_uuid': post_uuid,
            'post_label': post_label,
            'membership_id': membership_id,
            'membership_type': membership_type,
            'message': f"Could not extract complete person data from Staatskalender"
        })
        return issues
    
    # Step 4: Find or create person in Dataspot
    dataspot_person = find_person_by_sk_id(dataspot_client, sk_person_id)
    
    # If person doesn't exist, create them
    if not dataspot_person:
        # Check if we should attempt to create the person
        if not sk_email and is_primary:
            issues.append({
                'type': 'person_mismatch_missing_email',
                'post_uuid': post_uuid,
                'post_label': post_label,
                'membership_id': membership_id,
                'membership_type': membership_type,
                'sk_first_name': sk_first_name,
                'sk_last_name': sk_last_name,
                'sk_person_id': sk_person_id,
                'message': f"Person {sk_first_name} {sk_last_name} missing email in Staatskalender"
            })
            # Even if email is missing, still try to create the person
        
        # Create the person
        person_creation_result = create_person_in_dataspot(
            dataspot_client, 
            sk_first_name, 
            sk_last_name, 
            sk_person_id,
            sk_email
        )
        
        if person_creation_result['success']:
            dataspot_person = person_creation_result['person']
            logging.info(f"Created new person: {sk_first_name} {sk_last_name} with SK ID: {sk_person_id}")
        else:
            issues.append({
                'type': 'person_creation_failed',
                'post_uuid': post_uuid,
                'post_label': post_label,
                'membership_id': membership_id,
                'membership_type': membership_type,
                'sk_first_name': sk_first_name,
                'sk_last_name': sk_last_name,
                'sk_person_id': sk_person_id,
                'message': f"Failed to create person: {person_creation_result['message']}"
            })
            return issues
    
    # Step 5: Update person if needed
    if dataspot_person:
        person_update_result = ensure_person_data_correct(
            dataspot_client, 
            dataspot_person, 
            sk_first_name, 
            sk_last_name, 
            sk_person_id
        )
        
        if person_update_result['updated']:
            logging.info(f"Updated person {dataspot_person.get('id')}: {person_update_result['message']}")
            if person_update_result['issue']:
                issues.append(person_update_result['issue'])
    
    # Step 6: Link post to person if not already linked
    if dataspot_person:
        link_result = ensure_post_linked_to_person(
            dataspot_client, 
            post_uuid, 
            post_label, 
            dataspot_person.get('id'), 
            f"{sk_first_name} {sk_last_name}",
            is_primary
        )
        
        if link_result['issue']:
            issues.append(link_result['issue'])
    
    return issues


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
    
    try:
        import requests  # Import here to avoid potential circular imports
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
    except Exception as e:
        logging.error(f"Error retrieving membership data: {str(e)}")
        return {
            'success': False,
            'data': None,
            'status_code': 500,
            'error': str(e)
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


def find_person_by_sk_id(dataspot_client: BaseDataspotClient, sk_person_id: str) -> Optional[Dict[str, Any]]:
    """
    Find a person in Dataspot by sk_person_id.
    
    Args:
        dataspot_client: The Dataspot client
        sk_person_id: Staatskalender person ID
        
    Returns:
        Person dict if found, None otherwise
    """
    # Query to find person by sk_person_id
    sql_query = f"""
    SELECT
      p.id,
      p.given_name AS given_name,
      p.family_name AS family_name,
      cp.value AS sk_person_id
    FROM
      person_view p
    JOIN
      customproperties_view cp ON p.id = cp.resource_id
    WHERE
      cp.name = 'sk_person_id'
      AND cp.value = '"{sk_person_id}"'
    """
    
    try:
        result = dataspot_client.execute_query_api(sql_query=sql_query)
        if result and len(result) > 0:
            # Get the first matching person
            person_data = result[0]
            
            # Get full person details
            person_url = f"{dataspot_client.base_url}/rest/{dataspot_client.database_name}/persons/{person_data['id']}"
            person_response = requests_get(
                url=person_url,
                headers=dataspot_client.auth.get_headers()
            )
            
            if person_response.status_code == 200:
                return person_response.json()
            
            # If we can't get full details, return the basic info
            return {
                'id': person_data['id'],
                'givenName': person_data['given_name'],
                'familyName': person_data['family_name'],
                'customProperties': {
                    'sk_person_id': person_data['sk_person_id'].strip('"') if person_data['sk_person_id'] else None
                }
            }
        
        return None
    except Exception as e:
        logging.error(f"Error finding person by SK ID: {str(e)}")
        return None


def create_person_in_dataspot(
    dataspot_client: BaseDataspotClient,
    given_name: str,
    family_name: str,
    sk_person_id: str,
    email: Optional[str] = None
) -> Dict[str, Any]:
    """
    Create a new person in Dataspot.
    
    Args:
        dataspot_client: The Dataspot client
        given_name: Person's first name
        family_name: Person's last name
        sk_person_id: Staatskalender person ID
        email: Person's email address (optional)
        
    Returns:
        Dict with creation result
    """
    person_url = f"{dataspot_client.base_url}/rest/{dataspot_client.database_name}/persons"
    
    # Prepare person data
    person_data = {
        "_type": "Person",
        "givenName": given_name,
        "familyName": family_name,
        "customProperties": {
            "sk_person_id": sk_person_id
        }
    }
    
    if email:
        person_data["email"] = email
    
    try:
        # Create the person
        response = requests_post(
            url=person_url,
            json=person_data,
            headers=dataspot_client.auth.get_headers()
        )
        
        if response.status_code in [200, 201]:
            person = response.json()
            return {
                'success': True,
                'person': person,
                'message': f"Created new person: {given_name} {family_name}"
            }
        else:
            return {
                'success': False,
                'message': f"Failed to create person. Status code: {response.status_code}"
            }
    except Exception as e:
        return {
            'success': False,
            'message': f"Error creating person: {str(e)}"
        }


def ensure_person_data_correct(
    dataspot_client: BaseDataspotClient,
    person: Dict[str, Any],
    sk_first_name: str,
    sk_last_name: str,
    sk_person_id: str
) -> Dict[str, Any]:
    """
    Ensure person data matches Staatskalender data and update if needed.
    
    Args:
        dataspot_client: The Dataspot client
        person: Person data from Dataspot
        sk_first_name: First name from Staatskalender
        sk_last_name: Last name from Staatskalender
        sk_person_id: Staatskalender person ID
        
    Returns:
        Dict with update result
    """
    person_id = person.get('id')
    current_first_name = person.get('givenName')
    current_last_name = person.get('familyName')
    current_sk_id = person.get('customProperties', {}).get('sk_person_id')
    
    updates_needed = []
    
    # Check if name or SK ID needs updating
    if current_first_name != sk_first_name:
        updates_needed.append(f"first name from '{current_first_name}' to '{sk_first_name}'")
    
    if current_last_name != sk_last_name:
        updates_needed.append(f"last name from '{current_last_name}' to '{sk_last_name}'")
    
    if current_sk_id != sk_person_id:
        updates_needed.append(f"SK ID from '{current_sk_id}' to '{sk_person_id}'")
    
    # If no updates needed, return early
    if not updates_needed:
        return {
            'updated': False,
            'message': "No updates needed",
            'issue': None
        }
    
    # Prepare update data
    update_data = {
        "_type": "Person"
    }
    
    if current_first_name != sk_first_name:
        update_data["givenName"] = sk_first_name
    
    if current_last_name != sk_last_name:
        update_data["familyName"] = sk_last_name
    
    if current_sk_id != sk_person_id:
        if "customProperties" not in update_data:
            update_data["customProperties"] = {}
        update_data["customProperties"]["sk_person_id"] = sk_person_id
    
    # Update the person
    person_url = f"{dataspot_client.base_url}/rest/{dataspot_client.database_name}/persons/{person_id}"
    
    try:
        response = requests_patch(
            url=person_url,
            json=update_data,
            headers=dataspot_client.auth.get_headers()
        )
        
        if response.status_code in [200, 201]:
            # Create an issue for tracking the change
            issue = {
                'type': 'person_data_updated',
                'person_uuid': person_id,
                'dataspot_first_name': current_first_name,
                'dataspot_last_name': current_last_name,
                'sk_first_name': sk_first_name,
                'sk_last_name': sk_last_name,
                'message': f"Updated person data: {', '.join(updates_needed)}",
                'remediation_attempted': True,
                'remediation_success': True
            }
            
            return {
                'updated': True,
                'message': f"Updated person data: {', '.join(updates_needed)}",
                'issue': issue
            }
        else:
            issue = {
                'type': 'person_update_failed',
                'person_uuid': person_id,
                'dataspot_first_name': current_first_name,
                'dataspot_last_name': current_last_name,
                'sk_first_name': sk_first_name,
                'sk_last_name': sk_last_name,
                'message': f"Failed to update person data. Status code: {response.status_code}",
                'remediation_attempted': True,
                'remediation_success': False
            }
            
            return {
                'updated': False,
                'message': f"Failed to update person. Status code: {response.status_code}",
                'issue': issue
            }
    except Exception as e:
        issue = {
            'type': 'person_update_error',
            'person_uuid': person_id,
            'message': f"Error updating person: {str(e)}",
            'remediation_attempted': True,
            'remediation_success': False
        }
        
        return {
            'updated': False,
            'message': f"Error updating person: {str(e)}",
            'issue': issue
        }


def ensure_post_linked_to_person(
    dataspot_client: BaseDataspotClient,
    post_uuid: str,
    post_label: str,
    person_uuid: str,
    person_name: str,
    is_primary: bool = True
) -> Dict[str, Any]:
    """
    Ensure post is linked to the person.
    
    Args:
        dataspot_client: The Dataspot client
        post_uuid: UUID of the post
        post_label: Label of the post
        person_uuid: UUID of the person
        person_name: Name of the person for logging
        is_primary: Whether this is the primary person for the post
        
    Returns:
        Dict with link result
    """
    # Get current assignments for the post
    assignments_url = f"{dataspot_client.base_url}/api/{dataspot_client.database_name}/posts/{post_uuid}/holdsPost"
    
    try:
        assignments_response = requests_get(
            url=assignments_url,
            headers=dataspot_client.auth.get_headers()
        )
        
        if assignments_response.status_code != 200:
            return {
                'linked': False,
                'message': f"Could not retrieve post assignments. Status code: {assignments_response.status_code}",
                'issue': {
                    'type': 'post_assignment_error',
                    'post_uuid': post_uuid,
                    'post_label': post_label,
                    'person_uuid': person_uuid,
                    'message': f"Could not retrieve post assignments. Status code: {assignments_response.status_code}"
                }
            }
        
        assignments_data = assignments_response.json()
        assigned_persons = assignments_data.get('_embedded', {}).get('persons', [])
        
        # Check if person is already assigned
        person_already_assigned = False
        for assigned_person in assigned_persons:
            if assigned_person.get('id') == person_uuid:
                person_already_assigned = True
                break
        
        # If person is already assigned, no action needed
        if person_already_assigned:
            return {
                'linked': True,
                'message': f"Person {person_name} already assigned to post {post_label}",
                'issue': None
            }
        
        # If this is primary and there are existing assignments, we may need to replace them
        if is_primary and assigned_persons:
            # In a more sophisticated implementation, we might want to:
            # 1. Check if any existing assignment has the correct SK person ID
            # 2. If not, potentially remove incorrect assignments
            # 3. Add the correct person
            # For now, we'll just add the new person alongside existing ones
            pass
        
        # Create the assignment
        assignment_url = f"{dataspot_client.base_url}/api/{dataspot_client.database_name}/holdsPost"
        
        assignment_data = {
            "holds_post": post_uuid,
            "resource_id": person_uuid
        }
        
        assignment_response = requests_post(
            url=assignment_url,
            json=assignment_data,
            headers=dataspot_client.auth.get_headers()
        )
        
        if assignment_response.status_code in [200, 201]:
            return {
                'linked': True,
                'message': f"Linked person {person_name} to post {post_label}",
                'issue': {
                    'type': 'post_assignment_created',
                    'post_uuid': post_uuid,
                    'post_label': post_label,
                    'person_uuid': person_uuid,
                    'person_name': person_name,
                    'message': f"Created assignment between post {post_label} and person {person_name}",
                    'remediation_attempted': True,
                    'remediation_success': True
                }
            }
        else:
            return {
                'linked': False,
                'message': f"Failed to link person to post. Status code: {assignment_response.status_code}",
                'issue': {
                    'type': 'post_assignment_failed',
                    'post_uuid': post_uuid,
                    'post_label': post_label,
                    'person_uuid': person_uuid,
                    'person_name': person_name,
                    'message': f"Failed to create assignment. Status code: {assignment_response.status_code}",
                    'remediation_attempted': True,
                    'remediation_success': False
                }
            }
            
    except Exception as e:
        return {
            'linked': False,
            'message': f"Error linking person to post: {str(e)}",
            'issue': {
                'type': 'post_assignment_error',
                'post_uuid': post_uuid,
                'post_label': post_label,
                'person_uuid': person_uuid,
                'message': f"Error creating assignment: {str(e)}",
                'remediation_attempted': True,
                'remediation_success': False
            }
        }
