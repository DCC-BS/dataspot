import logging
from typing import Dict, List, Tuple

import config
from src.common import requests_patch
from src.clients.base_client import BaseDataspotClient


def check_3_post_assignment(dataspot_client: BaseDataspotClient, post_person_mapping__should: List[Tuple[str, str]]) -> Dict[str, any]:
    """
    Check #3: Mitgliedschaftsbasierte Posten-Zuordnungen
    
    This check verifies that all posts with membership IDs have correct person assignments based on Staatskalender data.
    
    IMPORTANT: This check MUST be run after check_2, as it requires the post_person_mapping__should
    from check_2's results. The mapping represents how posts SHOULD be assigned to persons according
    to Staatskalender data.
    
    Specifically:
    - For all posts with membership_id, it checks:
        - The person from the membership_id is correctly assigned to the post
        - The person from the second_membership_id is correctly assigned to the post
        - Only persons with valid membership IDs are assigned to the post
    - Both primary and secondary membership IDs are considered
    
    If not:
    - If a person is not assigned to the post that should be assigned, the assignment is automatically made
    - If other persons are assigned to the post, they are removed (only for posts with membership IDs)
    - All changes are documented in the report
    
    Args:
        dataspot_client: Base client for database operations
        post_person_mapping__should: Mapping of post_uuid to person_uuid tuples from check_2.
                                   This mapping represents how posts SHOULD be
                                   assigned to persons according to Staatskalender data. The list may
                                   be empty if no mappings should exist for posts with membership IDs.
        
    Returns:
        dict: Check results including status, issues, and any errors
    """
    logging.debug("Starting Check #3: Mitgliedschaftsbasierte Posten-Zuordnungen...")
    
    result = {
        'status': 'success',
        'message': 'All posts with membership IDs have correct person assignments.',
        'issues': [],
        'error': None
    }

    custom_approach = True
    if custom_approach:
        process_post_assignments_alternative(result, dataspot_client, post_person_mapping__should)


    else:    
        try:
            # Get all posts with membership_id or second_membership_id
            posts_with_membership_ids = get_posts_with_membership_ids(dataspot_client)
            
            if not posts_with_membership_ids:
                result['message'] = 'No posts with membership IDs found.'
                logging.info("No posts with membership IDs found.")
                return result
            
            logging.info(f"Found {len(posts_with_membership_ids)} posts with membership IDs to verify assignments")
            
            # Process and verify post assignments
            process_post_assignments(posts_with_membership_ids, dataspot_client, result)
            
            # Update final status and message
            if result['issues']:
                issue_count = len(result['issues'])
                remediated_count = sum(1 for issue in result['issues'] 
                                    if issue.get('remediation_attempted', False) 
                                    and issue.get('remediation_success', False))
                actual_issues = issue_count - remediated_count
                
                if actual_issues > 0:
                    result['status'] = 'warning'
                    result['message'] = f"Check #3: Found {issue_count} issue(s) ({remediated_count} automatically fixed, {actual_issues} requiring attention)"
                else:
                    result['message'] = f"Check #3: Fixed {remediated_count} issue(s), all posts have correct person assignments"
        
        except Exception as e:
            result['status'] = 'error'
            result['error'] = str(e)
            result['message'] = f"Error in Check #3 (Mitgliedschaftsbasierte Posten-Zuordnungen): {str(e)}"
            logging.error(f"Error in Check #3 (Mitgliedschaftsbasierte Posten-Zuordnungen): {str(e)}", exc_info=True)
        
    return result


def process_post_assignments_alternative(result: Dict[str, any], dataspot_client: BaseDataspotClient, post_person_mapping__should: List[Tuple[str, str]]) -> None:
    """
    Process post assignments using the mapping-based approach.
    
    It then:
    1. Adds missing assignments (persons in SHOULD but not in IS)
    2. Removes invalid assignments (persons in IS but not in SHOULD)
    
    The SHOULD assignments come from check_2 and represent the assignments according to
    Staatskalender data. This function ensures that the actual assignments in the system
    match what they should be.
    
    Args:
        post_person_mapping__should: Expected post assignments based on the mapping
        result: Result dictionary to update with issues
        dataspot_client: Database client
        
    Returns:
        None (updates the result dictionary in-place)
    """
    logging.info("Processing post assignments using mapping-based approach")

    # Retrieve current post assignments from dataspot
    query = """
            -- IS --
            SELECT
                p.id as person_uuid,
                p.given_name as first_name,
                p.family_name as last_name,
                post.label as post_label,
                post.id as post_uuid
            FROM
                person_view p
            JOIN
                holdspost_view hp ON p.id = hp.resource_id
            JOIN
                post_view post ON post.id = hp.holds_post;
            """
    result_is = dataspot_client.execute_query_api(sql_query=query)

    logging.info(f"Found {len(result_is)} persons in the IS")

    # Convert IS and SHOULD to more usable formats for comparison: dict of person_uuid to a list of post_uuids (list))
    is_assignments = {}
    for assignment in result_is:
        person_uuid = assignment['person_uuid']
        post_uuid = assignment['post_uuid']
        if person_uuid not in is_assignments:
            is_assignments[person_uuid] = []
        is_assignments[person_uuid].append(post_uuid)

    should_assignments = {}
    for post_uuid, person_uuid in post_person_mapping__should:
        if person_uuid not in should_assignments:
            should_assignments[person_uuid] = []
        should_assignments[person_uuid].append(post_uuid)

    posts_to_consider = get_posts_with_membership_ids(dataspot_client)

    # Create a mapping of person_uuid to their name to use for logging
    person_names_mapping = {}
    query = """
            SELECT
                p.id as person_uuid,
                p.given_name as first_name,
                p.family_name as last_name
            FROM
                person_view p
            """
    result_names = dataspot_client.execute_query_api(sql_query=query)
    for name in result_names:
        person_uuid = name['person_uuid']
        person_name = f"{name['first_name']} {name['last_name']}"
        person_names_mapping[person_uuid] = person_name


    # Update assignments in dataspot

    # Iterate through the person_uuids of the should_assignments
    for person_uuid in should_assignments:
        post_uuids = should_assignments[person_uuid]

        # Filter posts to only include those with membership_ids
        valid_post_uuids = [post_uuid for post_uuid in post_uuids if post_uuid in posts_to_consider]
        if not valid_post_uuids:
            continue

        # Determine if updates are required
        current_post_uuids = is_assignments.get(person_uuid, [])
        
        # Find posts to add and remove
        posts_to_add = [post_uuid for post_uuid in valid_post_uuids if post_uuid not in current_post_uuids]
        posts_to_remove = [post_uuid for post_uuid in current_post_uuids if post_uuid in posts_to_consider and post_uuid not in valid_post_uuids]
        
        # If no update is required, just log it and continue
        if not posts_to_add and not posts_to_remove:
            logging.info(f"No updates required for person {person_uuid}")
            continue
            
        # Log the planned changes at debug level
        if posts_to_add:
            logging.debug(f"Need to add {len(posts_to_add)} post(s) to person {person_uuid}: {posts_to_add}")
        if posts_to_remove:
            logging.debug(f"Need to remove {len(posts_to_remove)} post(s) from person {person_uuid}: {posts_to_remove}")
        
        # Try to update the person through REST API
        try:
            # Get person name from the person_names_mapping dictionary
            person_name = person_names_mapping[person_uuid]
            
            if person_name == "Unknown Person":
                logging.debug(f"Person {person_uuid} not found in current assignments, using UUID as identifier")
            
            # Process posts to add
            for post_uuid in posts_to_add:
                success = assign_person_to_post(dataspot_client, post_uuid, person_uuid)
                
                # Get post label for the issue
                post_label, _ = posts_to_consider[post_uuid]
                
                if success:
                    result['issues'].append({
                        'type': 'person_assignment_added',
                        'post_uuid': post_uuid,
                        'post_label': post_label or "Unknown post",
                        'person_uuid': person_uuid,
                        'person_name': person_name,
                        'message': f"Person {person_name} has been assigned to post {post_label or post_uuid}",
                        'remediation_attempted': True,
                        'remediation_success': True
                    })
                    logging.info(f"Added assignment: {person_name} -> {post_label or post_uuid}")
                else:
                    result['issues'].append({
                        'type': 'person_assignment_failed',
                        'post_uuid': post_uuid,
                        'post_label': post_label or "Unknown post",
                        'person_uuid': person_uuid,
                        'person_name': person_name,
                        'message': f"Failed to assign person {person_name} to post {post_label or post_uuid}",
                        'remediation_attempted': True,
                        'remediation_success': False
                    })
                    logging.error(f"Failed to add assignment: {person_name} -> {post_label or post_uuid}")
                    
            # Process posts to remove
            for post_uuid in posts_to_remove:
                success = remove_person_from_post(dataspot_client, post_uuid, person_uuid)
                
                # Get post label for the issue
                post_label, _ = posts_to_consider[post_uuid]
                
                if success:
                    result['issues'].append({
                        'type': 'person_assignment_removed',
                        'post_uuid': post_uuid,
                        'post_label': post_label or "Unknown post",
                        'person_uuid': person_uuid,
                        'person_name': person_name,
                        'message': f"Removed assignment of {person_name} from post {post_label or post_uuid}",
                        'remediation_attempted': True,
                        'remediation_success': True
                    })
                    logging.info(f"Removed assignment: {person_name} from {post_label or post_uuid}")
                else:
                    result['issues'].append({
                        'type': 'person_removal_failed',
                        'post_uuid': post_uuid,
                        'post_label': post_label or "Unknown post",
                        'person_uuid': person_uuid,
                        'person_name': person_name,
                        'message': f"Failed to remove assignment of {person_name} from post {post_label or post_uuid}",
                        'remediation_attempted': True,
                        'remediation_success': False
                    })
                    logging.error(f"Failed to remove assignment: {person_name} from {post_label or post_uuid}")
                    
        except Exception as e:
            logging.error(f"Error updating post assignments for person {person_uuid}: {str(e)}", exc_info=True)

    # Update final status and message based on issues
    if result['issues']:
        issue_count = len(result['issues'])
        remediated_count = sum(1 for issue in result['issues'] 
                            if issue.get('remediation_attempted', False) 
                            and issue.get('remediation_success', False))
        actual_issues = issue_count - remediated_count
        
        if actual_issues > 0:
            result['status'] = 'warning'
            result['message'] = f"Check #3: Found {issue_count} issue(s) ({remediated_count} automatically fixed, {actual_issues} requiring attention)"
        else:
            result['message'] = f"Check #3: Fixed {remediated_count} issue(s), all posts have correct person assignments"
    else:
        result['message'] = "Check #3: All posts have correct person assignments"
            
    logging.info(f"Found {len(is_assignments)} posts with current assignments")
    logging.info(f"Found {len(should_assignments)} posts with expected assignments")


def process_post_assignments(posts_with_membership_ids: Dict[str, Tuple[str, List[str]]], dataspot_client: BaseDataspotClient,
                             result: Dict[str, any]) -> None:
    """
    Process and correct post assignments based on Staatskalender data.
    
    This function assumes that Check #2 has already:
    - Validated all membership IDs against Staatskalender
    - Created/updated all persons in Dataspot
    - Set correct sk_person_id values for all persons

    Args:
        posts_with_membership_ids: Posts data with membership information
        dataspot_client: Database client
        result: Result dictionary to update with issues

    Returns:
        None (updates the result dictionary)
    """
    # Get all membership_id -> person UUID mappings in one query
    membership_to_person_mapping = get_membership_to_person_mapping(dataspot_client)
    
    total_posts = len(posts_with_membership_ids)

    for current_post, (post_uuid, (post_label, memberships)) in enumerate(posts_with_membership_ids.items(), 1):
        logging.info(f"[{current_post}/{total_posts}] Processing post: {post_label}")

        # Get current assignments for this post
        current_assignments = get_post_assignments(dataspot_client, post_uuid)
        
        # Get valid person UUIDs that should be assigned to this post
        valid_person_uuids = []
        
        # Process each membership ID to find the corresponding person
        for membership_id in memberships:
            # Get person info from the mapping (Check #2 already validated this membership_id)
            if membership_id not in membership_to_person_mapping:
                # This shouldn't happen if Check #2 ran successfully, but log it just in case
                logging.warning(f"  - Membership ID {membership_id} not found in mapping (Check #2 should have created this person)")
                continue
            
            person_info = membership_to_person_mapping[membership_id]
            person_uuid = person_info['person_uuid']
            person_name = f"{person_info['given_name']} {person_info['family_name']}"
            
            # Add to list of valid person UUIDs for this post
            valid_person_uuids.append(person_uuid)
            
            # Check if person is already assigned to the post
            person_assigned = any(a['person_uuid'] == person_uuid for a in current_assignments)
            
            if not person_assigned:
                # Person should be assigned but isn't - assign them
                success = assign_person_to_post(dataspot_client, post_uuid, person_uuid)
                
                if success:
                    result['issues'].append({
                        'type': 'person_assignment_added',
                        'post_uuid': post_uuid,
                        'post_label': post_label,
                        'person_uuid': person_uuid,
                        'person_name': person_name,
                        'membership_id': membership_id,
                        'message': f"Person {person_name} has been assigned to post {post_label}",
                        'remediation_attempted': True,
                        'remediation_success': True
                    })
                    logging.info(f"  - Added assignment: {person_name} -> {post_label}")
                else:
                    result['issues'].append({
                        'type': 'person_assignment_failed',
                        'post_uuid': post_uuid,
                        'post_label': post_label,
                        'person_uuid': person_uuid,
                        'person_name': person_name,
                        'membership_id': membership_id,
                        'message': f"Failed to assign person {person_name} to post {post_label}",
                        'remediation_attempted': True,
                        'remediation_success': False
                    })
                    logging.info(f"  - Failed to add assignment: {person_name} -> {post_label}")
            else:
                logging.info(f"  - Already assigned: {person_name} -> {post_label}")

        # Remove invalid assignments (persons assigned to post but not in Staatskalender)
        for assignment in current_assignments:
            if assignment['person_uuid'] not in valid_person_uuids:
                # Remove this person from the post
                success = remove_person_from_post(dataspot_client, post_uuid, assignment['person_uuid'])
                
                if success:
                    result['issues'].append({
                        'type': 'person_assignment_removed',
                        'post_uuid': post_uuid,
                        'post_label': post_label,
                        'person_uuid': assignment['person_uuid'],
                        'person_name': f"{assignment['given_name']} {assignment['family_name']}",
                        'message': f"Removed invalid assignment of {assignment['given_name']} {assignment['family_name']} from post {post_label}",
                        'remediation_attempted': True,
                        'remediation_success': True
                    })
                    logging.info(f"  - Removed invalid assignment: {assignment['given_name']} {assignment['family_name']} from {post_label}")
                else:
                    result['issues'].append({
                        'type': 'person_removal_failed',
                        'post_uuid': post_uuid,
                        'post_label': post_label,
                        'person_uuid': assignment['person_uuid'],
                        'person_name': f"{assignment['given_name']} {assignment['family_name']}",
                        'message': f"Failed to remove invalid assignment of {assignment['given_name']} {assignment['family_name']} from post {post_label}",
                        'remediation_attempted': True,
                        'remediation_success': False
                    })
                    logging.info(f"  - Failed to remove invalid assignment: {assignment['given_name']} {assignment['family_name']} from {post_label}")


def get_posts_with_membership_ids(dataspot_client: BaseDataspotClient) -> Dict[str, Tuple[str, List[str]]]:
    """
    Retrieve all posts that have membership IDs assigned.
    
    Args:
        dataspot_client: Database client
        
    Returns:
        dict: Posts with membership IDs (key: post_uuid, value: (post_label, [membership_ids]))
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


def get_post_assignments(dataspot_client: BaseDataspotClient, post_uuid: str) -> List[Dict[str, any]]:
    """
    Get current person assignments for a specific post.
    
    Args:
        dataspot_client: Database client
        post_uuid: UUID of the post to check
        
    Returns:
        list: Persons assigned to the post
    """
    query = f"""
    SELECT
        p.id AS person_uuid,
        p.given_name,
        p.family_name,
        cp.value AS sk_person_id
    FROM
        person_view p
    JOIN
        holdspost_view hp ON p.id = hp.resource_id
    LEFT JOIN
        customproperties_view cp ON p.id = cp.resource_id AND cp.name = 'sk_person_id'
    WHERE
        hp.holds_post = '{post_uuid}'
    """
    
    return dataspot_client.execute_query_api(sql_query=query)


def get_membership_to_person_mapping(dataspot_client: BaseDataspotClient) -> Dict[str, Dict[str, any]]:
    """
    Get all membership_id -> person UUID mappings from Dataspot.
    
    This function assumes that Check #2 has already created/updated all persons
    and set their sk_person_id values correctly.
    
    What we want to return:
    For each post that has a membership_id custom property, we want to find the person 
    who should be assigned to that post. We do this by:
    1. Finding posts with membership_id custom properties
    2. Finding the person who has a sk_person_id custom property that matches the membership_id
    3. Returning the mapping from membership_id to person information
    
    This mapping is used to verify that the correct person is assigned to each post
    based on the Staatskalender data.
    
    Args:
        dataspot_client: Database client
        
    Returns:
        dict: Mapping from membership_id to person info (person_uuid, given_name, family_name)
    """
    query = """
    SELECT 
        cp_post.value AS membership_id,
        p.id AS person_uuid,
        p.given_name,
        p.family_name
    FROM 
        customproperties_view cp_post
    JOIN 
        customproperties_view cp_person ON cp_post.value = cp_person.value
    JOIN 
        person_view p ON cp_person.resource_id = p.id
    WHERE 
        cp_post.name = 'membership_id' 
        AND cp_person.name = 'sk_person_id'
        AND cp_post.value IS NOT NULL
    """
    
    results = dataspot_client.execute_query_api(sql_query=query)
    mapping = {}
    
    for result in results:
        membership_id = result['membership_id'].strip('"')
        mapping[membership_id] = {
            'person_uuid': result['person_uuid'],
            'given_name': result['given_name'],
            'family_name': result['family_name']
        }
    
    return mapping


def assign_person_to_post(dataspot_client: BaseDataspotClient, post_uuid: str, person_uuid: str) -> bool:
    """
    Assign a person to a post.
    
    Args:
        dataspot_client: Database client
        post_uuid: UUID of the post
        person_uuid: UUID of the person to assign
        
    Returns:
        bool: True if successful, False otherwise
    """
    try:
        # Create holdsPost relationship
        holds_post_url = f"{dataspot_client.base_url}/rest/{dataspot_client.database_name}/persons/{person_uuid}"
        
        payload = {
            "holdsPost": post_uuid
        }
        
        response = requests_patch(
            url=holds_post_url,
            json=payload,
            headers=dataspot_client.auth.get_headers()
        )
        
        response.raise_for_status()
        return True
    
    except Exception as e:
        logging.error(f"Error assigning person to post: {str(e)}")
        return False


def remove_person_from_post(dataspot_client: BaseDataspotClient, post_uuid: str, person_uuid: str) -> bool:
    """
    Remove a person from a post.
    
    Args:
        dataspot_client: Database client
        post_uuid: UUID of the post
        person_uuid: UUID of the person to remove
        
    Returns:
        bool: True if successful, False otherwise
    """
    try:
        # Delete holdsPost relationship
        url = f"{dataspot_client.base_url}/rest/{dataspot_client.database_name}/persons/{person_uuid}/holdsPost/{post_uuid}"
        
        headers = dataspot_client.auth.get_headers()
        headers['Content-Type'] = 'application/json'
        
        response = requests_patch(
            url=url,
            json={"deleted": True},
            headers=headers
        )
        
        response.raise_for_status()
        return True
    
    except Exception as e:
        logging.error(f"Error removing person from post: {str(e)}")
        return False

