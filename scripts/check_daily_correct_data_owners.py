import logging
import traceback
from typing import Any, Dict

import config
from src.clients.base_client import BaseDataspotClient
from src.common import requests_get

def check_correct_data_owners(dataspot_client: BaseDataspotClient) -> Dict[str, Any]:
    """
    Check if all Data Owner posts are assigned to the correct person according to Staatskalender.

    This method:
    1. Executes a SQL query to find all Data Owner posts
    2. For each post:
       - Checks if it has a membership_id
       - Verifies the membership exists in Staatskalender
       - Compares the person in Dataspot with the person in Staatskalender
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

            # Track issues
            issues_count = 0

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
                    issues_count += 1
                    continue

                post_data = post_response.json().get('asset', {})

                # Step 2: Check if post has membership_id
                membership_id = post_data.get('customProperties', {}).get('membership_id')
                if not membership_id:
                    # Log the issue immediately
                    logging.warning(f"MISSING MEMBERSHIP: {dataspot_client.base_url}/web/{dataspot_client.database_name}/posts/{post_uuid})")

                    issue = {
                        'type': 'missing_membership',
                        'post_uuid': post_uuid,
                        'post_label': post_label,
                        'message': f"Post does not have a membership_id"
                    }
                    check_results['issues'].append(issue)
                    issues_count += 1
                    continue

                # Step 3: Check if membership exists in Staatskalender
                membership_url = f"https://staatskalender.bs.ch/api/memberships/{membership_id}"
                logging.info(f"Checking membership_id {membership_id} in Staatskalender...")
                logging.info(f"Retrieving membership data from Staatskalender: {membership_url}")
                membership_response = requests_get(url=membership_url)

                if membership_response.status_code != 200:
                    # Log the invalid membership ID immediately
                    logging.warning \
                        (f"INVALID MEMBERSHIP: Post '{post_label}' (UUID: {post_uuid}) has invalid membership_id '{membership_id}'. Status code: {membership_response.status_code}")

                    issue = {
                        'type': 'invalid_membership',
                        'post_uuid': post_uuid,
                        'post_label': post_label,
                        'membership_id': membership_id,
                        'message': f"Membership ID not found in Staatskalender. Status code: {membership_response.status_code}"
                    }
                    check_results['issues'].append(issue)
                    issues_count += 1
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
                        issues_count += 1
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
                        issues_count += 1
                        continue

                    # Extract first and last name from Staatskalender person
                    person_staatskalender_data = person_staatskalender_response.json()
                    sk_first_name = None
                    sk_last_name = None

                    for item in person_staatskalender_data.get('collection', {}).get('items', []):
                        for data_item in item.get('data', []):
                            if data_item.get('name') == 'first_name':
                                sk_first_name = data_item.get('value')
                            elif data_item.get('name') == 'last_name':
                                sk_last_name = data_item.get('value')
                            elif data_item.get('name') == 'email':
                                # TODO: Use email for User creation in dataspot
                                pass

                    if not sk_first_name or not sk_last_name:
                        issue = {
                            'type': 'missing_person_name',
                            'post_uuid': post_uuid,
                            'post_label': post_label,
                            'membership_id': membership_id,
                            'message': f"Could not extract name from Staatskalender person data"
                        }
                        check_results['issues'].append(issue)
                        issues_count += 1
                        continue

                    # Step 5: Find associated person in Dataspot
                    # Get a list of persons holding this post
                    person_query_url = f"{config.base_url}/api/{config.database_name}/posts/{post_uuid}/agentOf"
                    person_query_response = requests_get(
                        url=person_query_url,
                        headers=dataspot_client.auth.get_headers()
                    )

                    if person_query_response.status_code != 200:
                        issue = {
                            'type': 'dataspot_person_error',
                            'post_uuid': post_uuid,
                            'post_label': post_label,
                            'membership_id': membership_id,
                            'message': f"Could not retrieve associated person from Dataspot. Status code: {person_query_response.status_code}"
                        }
                        check_results['issues'].append(issue)
                        issues_count += 1
                        continue

                    # Check if any person holds this post
                    person_data = person_query_response.json()
                    if not person_data or '_embedded' not in person_data or 'persons' not in person_data.get \
                            ('_embedded', {}):
                        issue = {
                            'type': 'no_person_assigned',
                            'post_uuid': post_uuid,
                            'post_label': post_label,
                            'membership_id': membership_id,
                            'sk_first_name': sk_first_name,
                            'sk_last_name': sk_last_name,
                            'message': f"No person assigned to this post in Dataspot"
                        }
                        check_results['issues'].append(issue)
                        issues_count += 1
                        continue

                    # Get the first person (should be only one)
                    dataspot_persons = person_data.get('_embedded', {}).get('persons', [])
                    if not dataspot_persons:
                        issue = {
                            'type': 'no_person_assigned',
                            'post_uuid': post_uuid,
                            'post_label': post_label,
                            'membership_id': membership_id,
                            'sk_first_name': sk_first_name,
                            'sk_last_name': sk_last_name,
                            'message': f"No person assigned to this post in Dataspot"
                        }
                        check_results['issues'].append(issue)
                        issues_count += 1
                        continue

                    # Get the person's details
                    dataspot_person = dataspot_persons[0]
                    dataspot_person_uuid = dataspot_person.get('id')

                    # Get detailed person information
                    person_detail_url = f"{config.base_url}/api/{config.database_name}/persons/{dataspot_person_uuid}"
                    person_detail_response = requests_get(
                        url=person_detail_url,
                        headers=dataspot_client.auth.get_headers()
                    )

                    if person_detail_response.status_code != 200:
                        issue = {
                            'type': 'dataspot_person_details_error',
                            'post_uuid': post_uuid,
                            'post_label': post_label,
                            'membership_id': membership_id,
                            'dataspot_person_uuid': dataspot_person_uuid,
                            'message': f"Could not retrieve person details from Dataspot. Status code: {person_detail_response.status_code}"
                        }
                        check_results['issues'].append(issue)
                        issues_count += 1
                        continue

                    # Extract person details
                    person_details = person_detail_response.json().get('asset', {})
                    dataspot_first_name = person_details.get('givenName')
                    dataspot_last_name = person_details.get('familyName')

                    # Compare names
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
                        issues_count += 1
                        continue

                    # Check if names match
                    if sk_first_name != dataspot_first_name or sk_last_name != dataspot_last_name:
                        issue = {
                            'type': 'name_mismatch',
                            'post_uuid': post_uuid,
                            'post_label': post_label,
                            'membership_id': membership_id,
                            'dataspot_person_uuid': dataspot_person_uuid,
                            'sk_first_name': sk_first_name,
                            'sk_last_name': sk_last_name,
                            'dataspot_first_name': dataspot_first_name,
                            'dataspot_last_name': dataspot_last_name,
                            'message': f"Person name mismatch: Staatskalender ({sk_first_name} {sk_last_name}) vs. Dataspot ({dataspot_first_name} {dataspot_last_name})"
                        }
                        check_results['issues'].append(issue)
                        issues_count += 1
                    else:
                        logging.info \
                            (f"✓ Data Owner post '{post_label}' has correct person assignment: {dataspot_first_name} {dataspot_last_name}")

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
                    issues_count += 1
                    logging.error(f"Error processing post {post_label}: {str(e)}")

            # Update check results based on issues found
            if issues_count == 0:
                check_results['status'] = 'success'
                check_results['message'] = "All Data Owner posts have correct person assignments."
            else:
                check_results['status'] = 'warning'
                check_results['message'] = f"Found {issues_count} issues with Data Owner posts."

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
