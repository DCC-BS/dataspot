import logging
import time
from typing import Dict, List, Tuple

import config
from src.common import requests_patch, requests_get
from src.clients.base_client import BaseDataspotClient
from src.staatskalender_cache import StaatskalenderCache

# Global cache for person data
_person_with_sk_id_cache = None
_person_cache = None

def check_2_staatskalender_assignment(dataspot_client: BaseDataspotClient, staatskalender_cache: StaatskalenderCache) -> Dict[str, any]:
    """
    Check #2: Personensynchronisation aus dem Staatskalender
    
    This check verifies that all persons from Staatskalender are correctly present in Dataspot.
    
    Specifically:
    - For all posts with sk_membership_id, it checks:
        - The sk_membership_id exists in Staatskalender
        - The person from Staatskalender is correctly present in Dataspot with correct name
        - The person has the correct sk_person_id set
    - Both primary and secondary membership IDs are considered
    
    If not:
    - If the sk_membership_id is invalid, it is reported without making changes
    - If the person does not exist in Dataspot, they are automatically created with data (name, sk_person_id) from Staatskalender
    - If the person exists but has wrong data, these are automatically updated (name, sk_person_id)
    - All changes are documented in the report
    
    Args:
        dataspot_client: Base client for database operations
        staatskalender_cache: Cache instance for Staatskalender API data
        
    Returns:
        dict: Check results including status, issues, any errors, and a mapping of post_uuid to person_uuid
              that shows how posts SHOULD be assigned to persons according to Staatskalender data.
              This mapping (staatskalender_post_person_mapping) is provided for reuse in check_3 to avoid
              redundant Staatskalender API calls.
    """
    logging.debug("Starting Check #2: Personensynchronisation aus dem Staatskalender...")
    
    result = {
        'status': 'success',
        'message': 'All persons from Staatskalender are correctly present in Dataspot.',
        'issues': [],
        'error': None,
        'staatskalender_post_person_mapping': []
    }

    try:
        # Initialize BaseDataspotClient
        base_dataspot_client = BaseDataspotClient(scheme_name="NOT_IN_USE",
                                                  scheme_name_short="404NotFound")
        
        # Get all posts with sk_membership_id or sk_second_membership_id
        posts_with_membership = get_posts_with_sk_membership_ids(dataspot_client)
        
        if not posts_with_membership:
            result['message'] = 'No posts with membership IDs found.'
            return result

        logging.info(f"Found {len(posts_with_membership)} posts with membership IDs to verify")

        # Process person synchronization from Staatskalender
        process_person_sync(posts_with_membership, base_dataspot_client, result, staatskalender_cache)

        # Update final status and message
        if result['issues']:
            issue_count = len(result['issues'])
            remediated_count = sum(1 for issue in result['issues'] 
                                  if issue.get('remediation_attempted', False) 
                                  and issue.get('remediation_success', False))
            actual_issues = issue_count - remediated_count
            
            if actual_issues > 0:
                result['status'] = 'warning'
                result['message'] = f"Check #2: Found {issue_count} issue(s) ({remediated_count} automatically fixed, {actual_issues} requiring attention)"
            else:
                result['message'] = f"Check #2: Fixed {remediated_count} issue(s), all persons are correctly synchronized"
    
    except Exception as e:
        result['status'] = 'error'
        result['error'] = str(e)
        result['message'] = f"Error in Check #2 (Personensynchronisation aus dem Staatskalender): {str(e)}"
        logging.error(f"Error in Check #2 (Personensynchronisation aus dem Staatskalender): {str(e)}", exc_info=True)
    
    return result


# DONE
def get_posts_with_sk_membership_ids(dataspot_client: BaseDataspotClient) -> Dict[str, Tuple[str, List[str]]]:
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
        cp1.value AS sk_membership_id,
        cp2.value AS sk_second_membership_id
    FROM
        post_view p
    LEFT JOIN
        customproperties_view cp1 ON p.id = cp1.resource_id AND cp1.name = 'sk_membership_id'
    LEFT JOIN
        customproperties_view cp2 ON p.id = cp2.resource_id AND cp2.name = 'sk_second_membership_id'
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
        sk_membership_id = membership.get('sk_membership_id')
        sk_second_membership_id = membership.get('sk_second_membership_id')

        memberships = []
        if sk_membership_id:
            memberships.append(sk_membership_id.strip('"'))
        if sk_second_membership_id:
            memberships.append(sk_second_membership_id.strip('"'))

        result_dict[post_uuid] = (post_label, memberships)

    return result_dict


def process_person_sync(posts: Dict[str, Tuple[str, List[str]]], dataspot_client: BaseDataspotClient, result: Dict[str, any], staatskalender_cache: StaatskalenderCache) -> None:
    """
    Process person synchronization from Staatskalender.
    
    For each post, for each sk_membership_id (primary and secondary) linked to the post:
    - Ensure that the sk_membership_id is valid, i.e. exists in the staatskalender
    - Ensure that a person with the correct name (first and last) exists in dataspot
    - Ensure that the person has the sk_person_id set correctly

    - Build a mapping of post_uuid to person_uuid for use in check_3

    Args:
        posts: Posts data with membership information
        dataspot_client: Database client
        result: Result dictionary to update with issues and staatskalender_post_person_mapping
        staatskalender_cache: Cache instance for Staatskalender API data

    Returns:
        None (updates the result dictionary)
    """
    total_posts = len(posts)
    
    for current_post, (post_uuid, (post_label, memberships)) in enumerate(posts.items(), 1):
        # Log post header with progress indicator
        logging.info(f"[{current_post}/{total_posts}] {post_label}:")
        
        for sk_membership_id in memberships:
            # Add a delay to prevent overwhelming the API
            time.sleep(5)

            # Retrieve membership and person data from staatskalender using cache
            try:
                # Get person info from Staatskalender using the sk_membership_id
                person_data = staatskalender_cache.get_person_by_membership(sk_membership_id)
                
                sk_person_id = person_data['person_id']
                sk_first_name = person_data['given_name']
                sk_additional_name = person_data.get('additional_name')
                sk_last_name = person_data['family_name']
                sk_email = person_data.get('email')

                if not sk_first_name or not sk_last_name:
                    # Missing essential person data
                    result['issues'].append({
                        'type': 'person_data_incomplete',
                        'post_uuid': post_uuid,
                        'post_label': post_label,
                        'sk_membership_id': sk_membership_id,
                        'message': f"Person data is incomplete in Staatskalender",
                        'remediation_attempted': False,
                        'remediation_success': False
                    })
                    logging.info(f' - Person data is incomplete in Staatskalender for {sk_membership_id}')
                    continue

                # Check if a person with this sk_person_id already exists in Dataspot
                person_with_corresponding_sk_person_id_already_exists, existing_first_name, existing_last_name, person_uuid = (
                    check_person_with_corresponding_sk_person_id_already_exists(dataspot_client, sk_person_id))

                # If a person with corresponding sk_person_id exists, check if name is correct
                if person_with_corresponding_sk_person_id_already_exists:
                    # Ensure that the name is correct
                    if sk_first_name != existing_first_name or sk_last_name != existing_last_name:
                        # Update name
                        try:
                            update_person_name(
                                dataspot_client=dataspot_client,
                                person_uuid=person_uuid,
                                given_name=sk_first_name,
                                additional_name=sk_additional_name,
                                family_name=sk_last_name
                            )
                            result['issues'].append({
                                'type': 'person_name_update',
                                'post_uuid': post_uuid,
                                'post_label': post_label,
                                'sk_membership_id': sk_membership_id,
                                'person_uuid': person_uuid,
                                'given_name': existing_first_name,
                                'family_name': existing_last_name,
                                'sk_first_name': sk_first_name,
                                'sk_last_name': sk_last_name,
                                'message': f"Person name updated from {existing_first_name} {existing_last_name} to {sk_first_name} {sk_last_name}",
                                'remediation_attempted': True,
                                'remediation_success': True
                            })
                            logging.info(f' - Updated person name from "{existing_first_name} {existing_last_name}" to "{sk_first_name} {sk_last_name}" (Link: {config.base_url}/web/{config.database_name}/persons/{person_uuid})')
                        except Exception as e:
                            result['issues'].append({
                                'type': 'person_name_update_failed',
                                'post_uuid': post_uuid,
                                'post_label': post_label,
                                'sk_membership_id': sk_membership_id,
                                'person_uuid': person_uuid,
                                'given_name': existing_first_name,
                                'family_name': existing_last_name,
                                'sk_first_name': sk_first_name,
                                'sk_last_name': sk_last_name,
                                'message': f"Failed to update person name from {existing_first_name} {existing_last_name} to {sk_first_name} {sk_last_name}: {str(e)}",
                                'remediation_attempted': True,
                                'remediation_success': False
                            })
                            logging.error(f' - Failed to update person name from "{existing_first_name} {existing_last_name}" to "{sk_first_name} {sk_last_name}": {str(e)}')

                    else:
                        logging.info(f' - Person already exists and has correct name: {sk_first_name} {sk_last_name}')

                    # Add to the staatskalender_post_person_mapping for use in check_3
                    result['staatskalender_post_person_mapping'].append((post_uuid, person_uuid))
                    logging.debug(f'   - Added mapping: Post {post_label} -> Person {sk_first_name} {sk_last_name}')

                else:
                    # No person with this sk_person_id exists, so we need to find or create a person with the correct name
                    # First, try to find an existing person with the correct name using our cache
                    person_exists, person_uuid = find_person_by_name(dataspot_client, sk_first_name, sk_last_name)
                    
                    if person_exists:
                        logging.info(f' - Found existing person {sk_first_name} {sk_last_name}')
                    else:
                        # Person doesn't exist, create it using the existing method
                        try:
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
                                    'sk_membership_id': sk_membership_id,
                                    'person_uuid': person_uuid,
                                    'sk_first_name': sk_first_name,
                                    'sk_last_name': sk_last_name,
                                    'message': f"Person {sk_first_name} {sk_last_name} was created in dataspot (Link: {config.base_url}/web/{config.database_name}/persons/{person_uuid})",
                                    'remediation_attempted': True,
                                    'remediation_success': True
                                })
                                logging.info(f' - Created new person {sk_first_name} {sk_last_name} (Link: {config.base_url}/web/{config.database_name}/persons/{person_uuid})')
                            else:
                                # Person was found but not newly created
                                logging.info(f' - Person {sk_first_name} {sk_last_name} already exists')
                        except Exception as e:
                            result['issues'].append({
                                'type': 'person_creation_failed',
                                'post_uuid': post_uuid,
                                'post_label': post_label,
                                'sk_membership_id': sk_membership_id,
                                'sk_first_name': sk_first_name,
                                'sk_last_name': sk_last_name,
                                'message': f"Failed to create person {sk_first_name} {sk_last_name}: {str(e)}",
                                'remediation_attempted': True,
                                'remediation_success': False
                            })
                            logging.error(f' - Failed to create person {sk_first_name} {sk_last_name}: {str(e)}')
                            continue  # Skip to next membership ID

                    # Now ensure that the person has the correct sk_person_id
                    try:
                        person_sk_id_updated = ensure_correct_person_sk_id(dataspot_client, person_uuid, sk_person_id)
                        if person_sk_id_updated:
                            result['issues'].append({
                                'type': 'person_sk_id_updated',
                                'post_uuid': post_uuid,
                                'post_label': post_label,
                                'sk_membership_id': sk_membership_id,
                                'person_uuid': person_uuid,
                                'sk_person_id': sk_person_id,
                                'sk_first_name': sk_first_name,
                                'sk_last_name': sk_last_name,
                                'message': f"Person sk_person_id updated to {sk_person_id}",
                                'remediation_attempted': True,
                                'remediation_success': True
                            })
                            logging.info(f'   - Updated sk_person_id to {sk_person_id} for {sk_first_name} {sk_last_name} (Link: {config.base_url}/web/{config.database_name}/persons/{person_uuid})')
                        else:
                            logging.info(f' - Person {sk_first_name} {sk_last_name} already has correct sk_person_id')
                    except Exception as e:
                        result['issues'].append({
                            'type': 'person_sk_id_update_failed',
                            'post_uuid': post_uuid,
                            'post_label': post_label,
                            'sk_membership_id': sk_membership_id,
                            'person_uuid': person_uuid,
                            'sk_person_id': sk_person_id,
                            'sk_first_name': sk_first_name,
                            'sk_last_name': sk_last_name,
                            'message': f"Failed to update person sk_person_id to {sk_person_id}: {str(e)}",
                            'remediation_attempted': True,
                            'remediation_success': False
                        })
                        logging.error(f'   - Failed to update sk_person_id to {sk_person_id} for {sk_first_name} {sk_last_name}: {str(e)}')
                        
                    # Add to the staatskalender_post_person_mapping for use in check_3
                    result['staatskalender_post_person_mapping'].append((post_uuid, person_uuid))
                    logging.debug(f'   - Added mapping: Post {post_label} -> Person {sk_first_name} {sk_last_name}')

            except Exception as e:
                # Handle API errors - could be invalid membership or network errors after retries
                error_message = str(e)
                if "Could not find person link" in error_message or "not found" in error_message.lower():
                    issue_type = 'invalid_membership'
                    message = f"Invalid membership ID {sk_membership_id} - not found in Staatskalender"
                else:
                    issue_type = 'person_data_retrieval_failed'
                    message = (
                        f"The system could not load person data for this membership. What to do:\n"
                        f"    • Check https://staatskalender.bs.ch/person/{sk_membership_id} — if it works, the membership ID was set to a person ID by mistake; correct the membership ID in this post.\n"
                        f"    • If that link does not work, check https://staatskalender.bs.ch/membership/{sk_membership_id} — if it also fails, the membership no longer exists in the Staatskalender; then either delete this post or update it with a valid membership ID."
                    )
                
                result['issues'].append({
                    'type': issue_type,
                    'post_uuid': post_uuid,
                    'post_label': post_label,
                    'sk_membership_id': sk_membership_id,
                    'message': message,
                    'remediation_attempted': False,
                    'remediation_success': False
                })
                logging.error(f"Error processing membership ID {sk_membership_id}: {error_message}")
                logging.error(f"Membership URL: https://staatskalender.bs.ch/membership/{sk_membership_id}")


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
def update_person_name(dataspot_client: BaseDataspotClient, person_uuid: str, given_name: str, family_name: str, additional_name: str = None) -> None:
    """
    Update the name of a person.

    Args:
        dataspot_client: Database client
        person_uuid: Person UUID to update
        given_name: Person's first name
        family_name: Person's last name(s)
        additional_name: Person's middle name(s)

    Returns:
        None
    """
    # Prepare person update data for name
    person_update = {
        "_type": "Person",
        "givenName": given_name,
        "additionalName": additional_name,
        "familyName": family_name
    }

    # Update person via REST API
    update_url = f"{config.base_url}/rest/{config.database_name}/persons/{person_uuid}"

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
def ensure_correct_person_sk_id(dataspot_client: BaseDataspotClient, person_uuid: str, sk_person_id: str) -> bool:
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
    person_url = f"{config.base_url}/rest/{config.database_name}/persons/{person_uuid}"
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
        logging.debug(f'   - sk_person_id already correctly set: {sk_person_id}')
        return False
    
    logging.debug(f'   - Updating sk_person_id from {person_data.get("sk_person_id")} to {sk_person_id}')

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
