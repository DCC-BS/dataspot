import logging
from typing import Dict, List, Tuple

import config
from src.common import requests_get, requests_patch
from src.clients.base_client import BaseDataspotClient

# Global cache for person data
_person_with_sk_id_cache = None
_person_cache = None

def check_2_staatskalender_assignment(dataspot_client: BaseDataspotClient) -> Dict[str, any]:
    """
    Check #2: Personensynchronisation aus dem Staatskalender
    
    This check verifies that all persons from Staatskalender are correctly present in Dataspot.
    
    Specifically:
    - For all posts with membership_id, it checks:
        - The membership_id exists in Staatskalender
        - The person from Staatskalender is correctly present in Dataspot with correct name
        - The person has the correct sk_person_id set
    - Both primary and secondary membership IDs are considered
    
    If not:
    - If the membership_id is invalid, it is reported without making changes
    - If the person does not exist in Dataspot, they are automatically created with data (name, sk_person_id) from Staatskalender
    - If the person exists but has wrong data, these are automatically updated (name, sk_person_id)
    - All changes are documented in the report
    
    Args:
        dataspot_client: Base client for database operations
        
    Returns:
        dict: Check results including status, issues, and any errors
    """
    logging.info("Starting Check #2: Personensynchronisation aus dem Staatskalender...")
    
    result = {
        'status': 'success',
        'message': 'All persons from Staatskalender are correctly present in Dataspot.',
        'issues': [],
        'error': None
    }
    
    try:
        # Initialize BaseDataspotClient
        base_dataspot_client = BaseDataspotClient(base_url=config.base_url,
                                                  database_name=config.database_name,
                                                  scheme_name="NOT_IN_USE",
                                                  scheme_name_short="404NotFound")
        
        # Get all posts with membership_id or second_membership_id
        posts_with_membership = get_posts_with_membership_ids(dataspot_client)
        
        if not posts_with_membership:
            result['message'] = 'No posts with membership IDs found.'
            return result

        logging.info(f"Found {len(posts_with_membership)} posts with membership IDs to verify")

        # Process person synchronization from Staatskalender
        process_person_sync(posts_with_membership, base_dataspot_client, result)

        # Update final status and message
        if result['issues']:
            issue_count = len(result['issues'])
            remediated_count = sum(1 for issue in result['issues'] 
                                  if issue.get('remediation_attempted', False) 
                                  and issue.get('remediation_success', False))
            actual_issues = issue_count - remediated_count
            
            if actual_issues > 0:
                result['status'] = 'warning'
                result['message'] = f"Check #2: Found {issue_count} issues ({remediated_count} automatically fixed, {actual_issues} requiring attention)"
            else:
                result['message'] = f"Check #2: Fixed {remediated_count} issues, all persons are correctly synchronized"
    
    except Exception as e:
        result['status'] = 'error'
        result['error'] = str(e)
        result['message'] = f"Error in Check #2 (Personensynchronisation aus dem Staatskalender): {str(e)}"
        logging.error(f"Error in Check #2 (Personensynchronisation aus dem Staatskalender): {str(e)}", exc_info=True)
    
    return result


# DONE
def get_posts_with_membership_ids(dataspot_client: BaseDataspotClient) -> Dict[str, Tuple[str, List[str]]]:
    """
    Retrieve all posts that have membership IDs assigned.
    
    Args:
        dataspot_client: Database client
        
    Returns:
        list: Posts with membership IDs
    """
    query = """
    SELECT
        p.id AS post_uuid,
        p.label AS post_label,
        cp1.value AS membership_id,
        cp2.value AS second_membership_id
    FROM
        post_view p
    LEFT JOIN
        customproperties_view cp1 ON p.id = cp1.resource_id AND cp1.name = 'membership_id'
    LEFT JOIN
        customproperties_view cp2 ON p.id = cp2.resource_id AND cp2.name = 'second_membership_id'
    WHERE
        cp1.value IS NOT NULL OR cp2.value IS NOT NULL
    ORDER BY
        p.label
    """

    query_result = dataspot_client.execute_query_api(sql_query=query)
    result_dict = dict()
    for membership in query_result:
        post_uuid = membership['post_uuid']
        post_label = membership['post_label']
        membership_id = membership.get('membership_id')
        second_membership_id = membership.get('second_membership_id')

        memberships = []
        if membership_id:
            memberships.append(membership_id.strip('"'))
        if second_membership_id:
            memberships.append(second_membership_id.strip('"'))

        result_dict[post_uuid] = (post_label, memberships)

    return result_dict


def process_person_sync(posts: Dict[str, Tuple[str, List[str]]], dataspot_client: BaseDataspotClient, result: Dict[str, any]) -> None:
    """
    Process person synchronization from Staatskalender.
    
    For each post, for each membership_id (primary and secondary) linked to the post:
    - Ensure that the membership_id is valid, i.e. exists in the staatskalender
    - Ensure that a person with the correct name (first and last) exists in dataspot
    - Ensure that the person has the sk_person_id set correctly

    Args:
        posts: Posts data with membership information
        dataspot_client: Database client
        result: Result dictionary to update with issues

    Returns:
        None (updates the result dictionary)
    """
    total_posts = len(posts)
    
    for current_post, (post_uuid, (post_label, memberships)) in enumerate(posts.items(), 1):
        # Log post header with progress indicator
        logging.info(f"[{current_post}/{total_posts}] {post_label}:")
        
        for membership_id in memberships:
            # Retrieve membership data from staatskalender
            try:
                # Get person info from Staatskalender using the membership_id
                membership_url = f"https://staatskalender.bs.ch/api/memberships/{membership_id}"
                membership_response = requests_get(url=membership_url)

                if membership_response.status_code != 200:
                    result['issues'].append({
                        'type': 'invalid_membership',
                        'post_uuid': post_uuid,
                        'post_label': post_label,
                        'membership_id': membership_id,
                        'message': f"Invalid membership ID {membership_id} - not found in Staatskalender",
                        'remediation_attempted': False,
                        'remediation_success': False
                    })
                    return

                # Extract person link from membership data
                membership_data = membership_response.json()
                person_link = None

                for item in membership_data.get('collection', {}).get('items', []):
                    for link in item.get('links', []):
                        if link.get('rel') == 'person':
                            person_link = link.get('href')
                            break
                    if person_link:
                        break

                if not person_link:
                    result['issues'].append({
                        'type': 'missing_person_link',
                        'post_uuid': post_uuid,
                        'post_label': post_label,
                        'membership_id': membership_id,
                        'message': f"Could not find person link in membership data",
                        'remediation_attempted': False,
                        'remediation_success': False
                    })
                    return

                # Get person data from Staatskalender
                person_response = requests_get(url=person_link)

                if person_response.status_code != 200:
                    result['issues'].append({
                        'type': 'person_data_error',
                        'post_uuid': post_uuid,
                        'post_label': post_label,
                        'membership_id': membership_id,
                        'message': f"Could not retrieve person data from Staatskalender. Status code: {person_response.status_code}",
                        'remediation_attempted': False,
                        'remediation_success': False
                    })
                    return

                # Extract person details
                person_data = person_response.json()
                sk_person_id = person_link.rsplit('/', 1)[1]
                sk_first_name = None
                sk_last_name = None

                for item in person_data.get('collection', {}).get('items', []):
                    for data_item in item.get('data', []):
                        if data_item.get('name') == 'first_name':
                            sk_first_name = data_item.get('value')
                        elif data_item.get('name') == 'last_name':
                            sk_last_name = data_item.get('value')

                        if sk_first_name and sk_last_name:
                            break

                if not sk_person_id or not sk_first_name or not sk_last_name:
                    # Missing essential person data
                    result['issues'].append({
                        'type': 'missing_person_data',
                        'post_uuid': post_uuid,
                        'post_label': post_label,
                        'membership_id': membership_id,
                        'message': f"Person data is incomplete in Staatskalender",
                        'remediation_attempted': False,
                        'remediation_success': False
                    })
                    return

                # Check if a person with this sk_person_id already exists in Dataspot
                person_with_corresponding_sk_person_id_already_exists, existing_first_name, existing_last_name, person_uuid = (
                    check_person_with_corresponding_sk_person_id_already_exists(dataspot_client, sk_person_id))

                # If a person with corresponding sk_person_id exists, check if name is correct
                if person_with_corresponding_sk_person_id_already_exists:
                    # Ensure that the name is correct
                    if sk_first_name != existing_first_name or sk_last_name != existing_last_name:
                        # Update name
                        update_person_name(dataspot_client=dataspot_client, person_uuid=person_uuid, given_name=sk_first_name,family_name=sk_last_name)
                        result['issues'].append({
                            'type': 'person_name_mismatch',
                            'post_uuid': post_uuid,
                            'post_label': post_label,
                            'membership_id': membership_id,
                            'message': f"Person name mismatch: {existing_first_name} {existing_last_name} -> {sk_first_name} {sk_last_name}",
                            'remediation_attempted': True,
                            'remediation_success': True
                        })
                        logging.info(f' - Updated person name from "{existing_first_name} {existing_last_name}" to "{sk_first_name} {sk_last_name}" (Link: {config.base_url}/web/{config.database_name}/persons/{person_uuid})')

                    else:
                        logging.info(f' - Person {sk_first_name} {sk_last_name} already exists and has correct name')

                else:
                    # No person with this sk_person_id exists, so we need to find or create a person with the correct name
                    # First, try to find an existing person with the correct name using our cache
                    person_exists, person_uuid = find_person_by_name(dataspot_client, sk_first_name, sk_last_name)
                    
                    if person_exists:
                        logging.info(f' - Found existing person {sk_first_name} {sk_last_name}')
                    else:
                        # Person doesn't exist, create it using the existing method
                        person_uuid, person_newly_created = dataspot_client.ensure_person_exists(sk_first_name, sk_last_name)
                        
                        if person_newly_created:
                            # Reset caches since a new person was created
                            global _person_with_sk_id_cache, _person_cache
                            _person_with_sk_id_cache = None
                            _person_cache = None

                            # Add remediation issue stating that the person was created
                            result['issues'].append({
                                'type': 'person_created',
                                'post_uuid': post_uuid,
                                'post_label': post_label,
                                'membership_id': membership_id,
                                'person_uuid': person_uuid,
                                'message': f"Person {sk_first_name} {sk_last_name} was created in dataspot (Link: {config.base_url}/web/{config.database_name}/persons/{person_uuid})",
                                'remediation_attempted': True,
                                'remediation_success': True
                            })
                            logging.info(f' - Created new person {sk_first_name} {sk_last_name} (Link: {config.base_url}/web/{config.database_name}/persons/{person_uuid})')

                    # Now ensure that the person has the correct sk_person_id
                    person_sk_id_updated = ensure_person_sk_id(dataspot_client, person_uuid, sk_person_id)
                    if person_sk_id_updated:
                        result['issues'].append({
                            'type': 'person_sk_id_updated',
                            'post_uuid': post_uuid,
                            'post_label': post_label,
                            'membership_id': membership_id,
                            'message': f"Person sk_person_id updated to {sk_person_id}",
                            'remediation_attempted': True,
                            'remediation_success': True
                        })
                        logging.info(f'   - Updated sk_person_id to {sk_person_id} for {sk_first_name} {sk_last_name} (Link: {config.base_url}/web/{config.database_name}/persons/{person_uuid})')
                    else:
                        logging.info(f' - Person {sk_first_name} {sk_last_name} already has correct sk_person_id')

            except Exception as e:
                result['issues'].append({
                    'type': 'processing_error',
                    'post_uuid': post_uuid,
                    'post_label': post_label,
                    'membership_id': membership_id,
                    'message': f"Error processing membership ID {membership_id}: {str(e)}",
                    'remediation_attempted': False,
                    'remediation_success': False
                })


# DONE
def check_person_with_corresponding_sk_person_id_already_exists(dataspot_client: BaseDataspotClient, sk_person_id: str) -> Tuple[bool, str, str, str]:
    """
    Checks if a person with the given Staatskalender ID already exists in the database.

    Args:
        dataspot_client: Database client
        sk_person_id: Staatskalender person ID

    Returns:
        Tuple[bool, str, str]: Tuple containing:
            - bool: True if a person with the given sk_person_id exists, False otherwise
            - str: First name of the person if found, "no_first_name" if not found
            - str: Last name of the person if found, "no_last_name" if not found
            - str: UUID of the person in dataspot, "no_person_uuid" if not found
    """
    global _person_with_sk_id_cache

    # Load cache if not already loaded
    if _person_with_sk_id_cache is None:
        logging.debug("Loading person cache...")
        query = """
        SELECT
            p.id,
            p.given_name,
            p.family_name,
            cp.value AS sk_person_id
        FROM
            person_view p
        JOIN
            customproperties_view cp ON p.id = cp.resource_id
        WHERE
            cp.name = 'sk_person_id'
        """
        results = dataspot_client.execute_query_api(sql_query=query)
        _person_with_sk_id_cache = {}
        for result in results:
            sk_id = result['sk_person_id'].strip('"')
            _person_with_sk_id_cache[sk_id] = (True, result['given_name'], result['family_name'], result['id'])
        logging.debug(f"Person with sk_person_id cache loaded with {len(_person_with_sk_id_cache)} entries")

    # Check if person exists in cache
    if sk_person_id in _person_with_sk_id_cache:
        return _person_with_sk_id_cache[sk_person_id]

    # Person not found in cache, return not found
    return False, "no_first_name", "no_last_name", "no_person_uuid"


def find_person_by_name(dataspot_client: BaseDataspotClient, first_name: str, last_name: str) -> Tuple[bool, str]:
    """
    Find a person by name using cached data.

    Args:
        dataspot_client: Database client
        first_name: Person's first name
        last_name: Person's last name

    Returns:
        Tuple[bool, str]: Tuple containing:
            - bool: True if a person with the given name exists, False otherwise
            - str: UUID of the person if found, "no_person_uuid" if not found
    """
    global _person_cache

    # Load cache if not already loaded
    if _person_cache is None:
        logging.debug("Loading all persons cache...")
        query = """
        SELECT
            p.id,
            p.given_name,
            p.family_name
        FROM
            person_view p
        ORDER BY
            p.family_name, p.given_name
        """
        results = dataspot_client.execute_query_api(sql_query=query)
        _person_cache = {}
        for result in results:
            person_name = f"{result['given_name']} {result['family_name']}"
            _person_cache[person_name] = result['id']
        logging.debug(f"All persons cache loaded with {len(_person_cache)} entries")

    # Check if person exists in cache
    person_name = f"{first_name} {last_name}"
    if person_name in _person_cache:
        return True, _person_cache[person_name]

    # Person not found in cache, return not found
    return False, "no_person_uuid"

# DONE
def update_person_name(dataspot_client: BaseDataspotClient, person_uuid: str, given_name: str, family_name: str) -> None:
    """
    Update the name of a person.

    Args:
        dataspot_client: Database client
        person_uuid: Person UUID to update
        given_name: Person's first name
        family_name: Person's last name

    Returns:
        None
    """
    # Prepare person update data for name
    person_update = {
        "_type": "Person",
        "givenName": given_name,
        "familyName": family_name
    }

    # Update person via REST API
    update_url = f"{dataspot_client.base_url}/rest/{dataspot_client.database_name}/persons/{person_uuid}"

    response = requests_patch(
        url=update_url,
        json=person_update,
        headers=dataspot_client.auth.get_headers()
    )

    response.raise_for_status()

    # Reset caches since person data was modified
    global _person_with_sk_id_cache, _person_cache
    _person_with_sk_id_cache = None
    _person_cache = None

# DONE
def ensure_person_sk_id(dataspot_client: BaseDataspotClient, person_uuid: str, sk_person_id: str) -> bool:
    """
    Ensure that a person has the correct Staatskalender ID.

    Args:
        dataspot_client: Database client
        person_uuid: Person UUID to update
        sk_person_id: Staatskalender person ID
        
    Returns:
        bool: True if the sk_person_id was updated, False otherwise
    """
    # First check if the property already exists using the REST API
    person_url = f"{dataspot_client.base_url}/rest/{dataspot_client.database_name}/persons/{person_uuid}"
    response = requests_get(
        url=person_url,
        headers=dataspot_client.auth.get_headers()
    )
    
    if response.status_code != 200:
        logging.error(f"Failed to retrieve person with UUID {person_uuid}. Status code: {response.status_code}")
        return False
    
    person_data = response.json()
    
    # Check if the sk_person_id is already correctly set
    if person_data.get('sk_person_id') == sk_person_id:
        return False
    
    # If not, update sk_person_id
    person_update = {
        "_type": "Person",
        "customProperties": {
            "sk_person_id": sk_person_id
        }
    }

    response = requests_patch(
        url=person_url,
        json=person_update,
        headers=dataspot_client.auth.get_headers()
    )

    response.raise_for_status()
    
    # Reset caches since person data was modified
    global _person_with_sk_id_cache, _person_cache
    _person_with_sk_id_cache = None
    _person_cache = None
    return True
