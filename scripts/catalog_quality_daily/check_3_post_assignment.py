import logging
from typing import Dict, List, Tuple

import config
from src.common import requests_get, requests_patch
from src.clients.base_client import BaseDataspotClient


def check_3_post_assignment(dataspot_client: BaseDataspotClient, staatskalender_post_person_mapping: List[Tuple[str, str]]) -> Dict[str, any]:
    """
    Check #3: Mitgliedschaftsbasierte Posten-Zuordnungen
    
    This check verifies that all posts with membership IDs (primary or secondary) have correct person
    assignments based on Staatskalender data.
    
    IMPORTANT: This check MUST be run after check_2, as it requires the staatskalender_post_person_mapping
    from check_2's results. The mapping represents how posts SHOULD be assigned to persons according
    to Staatskalender data.
    
    Specifically:
    - For all posts with membership_id, it checks:
        - The person from the sk_membership_id is correctly assigned to the post
        - The person from the sk_second_membership_id is correctly assigned to the post
        - Only persons with valid membership IDs are assigned to the post
    - Both primary and secondary membership IDs are considered
    
    If not:
    - If a person is not assigned to the post that should be assigned, the assignment is automatically made
    - If other persons are assigned to the post, they are removed (only for posts with membership IDs)
    - All changes are documented in the report
    
    Args:
        dataspot_client: Base client for database operations
        staatskalender_post_person_mapping: Mapping of post_uuid to person_uuid tuples from check_2.
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

    logging.info(f"Found {len(result_is)} assignments in the IS")

    # Convert IS and SHOULD to more usable formats for comparison: dict of person_uuid to a list of post_uuids (list)
    is_assignments = {}
    for assignment in result_is:
        person_uuid = assignment['person_uuid']
        post_uuid = assignment['post_uuid']
        if person_uuid not in is_assignments:
            is_assignments[person_uuid] = []
        is_assignments[person_uuid].append(post_uuid)

    should_assignments = {}
    for post_uuid, person_uuid in staatskalender_post_person_mapping:
        if person_uuid not in should_assignments:
            should_assignments[person_uuid] = []
        should_assignments[person_uuid].append(post_uuid)

    posts_to_consider = get_posts_with_sk_membership_ids(dataspot_client)

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

    # Process each person who has or should have posts with membership_ids
    for person_uuid in set(list(should_assignments.keys()) + list(is_assignments.keys())):
        # Get person name for logs
        person_name = person_names_mapping[person_uuid]

        # Get current posts with membership_ids
        current_posts = [p for p in is_assignments.get(person_uuid, []) if p in posts_to_consider]

        # Get posts that should be assigned (filtered to only those with membership_ids)
        should_have_posts = [p for p in should_assignments.get(person_uuid, []) if p in posts_to_consider]

        # Only process if person has or should have posts with membership_ids
        if current_posts or should_have_posts:
            # Get all current posts, including those not in posts_to_consider
            all_current_posts = is_assignments.get(person_uuid, [])

            # Calculate desired posts - only consider posts in posts_to_consider for changes
            # Start with all current posts EXCEPT those in posts_to_consider that should be removed
            desired_posts = [p for p in all_current_posts if p not in posts_to_consider or p in should_have_posts]

            # Now add any should_have posts that aren't already in the list
            for post in should_have_posts:
                if post not in desired_posts:
                    desired_posts.append(post)

            # Calculate posts to add and remove for logging and issue tracking
            posts_to_add = [p for p in should_have_posts if p not in current_posts]
            posts_to_remove = [p for p in current_posts if p not in should_have_posts]

            # Only update if changes are needed
            if posts_to_add or posts_to_remove:
                logging.debug(f"Person {person_name}: adding {len(posts_to_add)} posts, removing {len(posts_to_remove)} posts")
                if posts_to_add:
                    logging.debug(f"Posts to add: {posts_to_add}")
                if posts_to_remove:
                    logging.debug(f"Posts to remove: {posts_to_remove}")

                # Update the person's posts
                try:
                    # Update posts in one call, passing current posts to avoid an extra API call
                    update_results = update_holds_post(
                        dataspot_client,
                        person_uuid,
                        desired_posts,
                        all_current_posts
                    )

                    # Process results - added posts
                    for post_uuid in update_results['added']:
                        post_label, _ = posts_to_consider[post_uuid]

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

                    # Process results - removed posts
                    for post_uuid in update_results['removed']:
                        post_label, _ = posts_to_consider[post_uuid]

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

                except Exception as e:
                    logging.error(f"Error updating posts for person {person_name}: {e}", exc_info=True)

                    # Log failures for each intended change
                    for post_uuid in posts_to_add:
                        post_label, _ = posts_to_consider.get(post_uuid, ("Unknown post", None))
                        result['issues'].append({
                            'type': 'person_assignment_add_failed',
                            'post_uuid': post_uuid,
                            'post_label': post_label or "Unknown post",
                            'person_uuid': person_uuid,
                            'person_name': person_name,
                            'message': f"Failed to assign person {person_name} to post {post_label or post_uuid}",
                            'remediation_attempted': True,
                            'remediation_success': False
                        })
                        logging.error(f"Failed to assign person {person_name} to post {post_label or post_uuid}")

                    for post_uuid in posts_to_remove:
                        post_label, _ = posts_to_consider.get(post_uuid, ("Unknown post", None))
                        result['issues'].append({
                            'type': 'person_assignment_remove_failed',
                            'post_uuid': post_uuid,
                            'post_label': post_label or "Unknown post",
                            'person_uuid': person_uuid,
                            'person_name': person_name,
                            'message': f"Failed to remove assignment of {person_name} from post {post_label or post_uuid}",
                            'remediation_attempted': True,
                            'remediation_success': False
                        })
                        logging.error(f"Failed to remove assignment of {person_name} from post {post_label or post_uuid}")
            else:
                logging.debug(f"No changes needed for person {person_name} (UUID: {person_uuid})")

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

    return result


def get_posts_with_sk_membership_ids(dataspot_client: BaseDataspotClient) -> Dict[str, Tuple[str, List[str]]]:
    """
    Retrieve all posts that have membership IDs (primary or secondary) assigned.
    
    Args:
        dataspot_client: Database client
        
    Returns:
        dict: Posts with membership IDs (key: post_uuid, value: (post_label, [sk_membership_ids]))
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


def update_holds_post(dataspot_client: BaseDataspotClient, person_uuid: str, post_uuids: List[str], current_posts: List[str]) -> Dict[str, List[str]]:
    """
    Update all posts that a person holds in one operation.
    
    This function updates the holdsPost relationship with the provided list and returns which
    posts were added or removed compared to the current_posts provided.
    
    Args:
        dataspot_client: Database client
        person_uuid: UUID of the person to update
        post_uuids: List of post UUIDs that the person should hold
        current_posts: List of post UUIDs the person currently holds
        
    Returns:
        dict: Results showing which posts were added and removed
              Format: {'added': [post_uuid1, post_uuid2], 'removed': [post_uuid3]}
    """
    # Determine posts to add and remove
    posts_to_add = [post for post in post_uuids if post not in current_posts]
    posts_to_remove = [post for post in current_posts if post not in post_uuids]
    
    # If no changes needed, return empty result
    if not posts_to_add and not posts_to_remove:
        return {'added': [], 'removed': []}
    
    result = {
        'added': posts_to_add,
        'removed': posts_to_remove
    }
    
    try:
        # Update holdsPost relationship
        person_url = f"{dataspot_client.base_url}/rest/{dataspot_client.database_name}/persons/{person_uuid}"
        
        payload = {
            "_type": 'Person',
            "holdsPost": post_uuids
        }
        
        update_response = requests_patch(
            url=person_url,
            json=payload,
            headers=dataspot_client.auth.get_headers()
        )
        
        update_response.raise_for_status()
        return result
    
    except Exception as e:
        logging.error(f"Error updating person's posts: {str(e)}")
        return {'added': [], 'removed': []}
