import logging
import traceback
from typing import Any, Dict

import config
from src.common import requests_get
from src.clients.base_client import BaseDataspotClient

def check_persons_without_users(dataspot_client: BaseDataspotClient) -> Dict[str, Any]:
    """
    Check if all persons in Dataspot have a corresponding user.

    This method:
    1. Retrieves all persons from Dataspot
    2. Retrieves all users from Dataspot
    3. Builds a mapping of persons to users via the isPerson field
    4. Identifies persons that don't have a corresponding user
    5. Reports these as issues

    Returns:
        Dict[str, Any]: Check results containing status, message, and issues
    """
    # Store results for reporting
    check_results = {
        'status': 'pending',
        'message': '',
        'issues': [],
        'error': None
    }

    try:
        # Step 1: Get all persons from Dataspot
        persons_url = f"{dataspot_client.base_url}/rest/{dataspot_client.database_name}/persons"
        persons_response = requests_get(
            url=persons_url,
            headers=dataspot_client.auth.get_headers()
        )

        if persons_response.status_code != 200:
            check_results['status'] = 'error'
            check_results['message'] = f"Failed to retrieve persons. Status code: {persons_response.status_code}"
            return check_results

        persons_data = persons_response.json()
        persons = persons_data.get('_embedded', {}).get('persons', [])

        # Step 2: Get all users from Dataspot
        users_url = f"{dataspot_client.base_url}/api/{dataspot_client.database_name}/tenants/{config.tenant_name}/download?format=JSON"
        users_response = requests_get(
            url=users_url,
            headers=dataspot_client.auth.get_headers()
        )

        if users_response.status_code != 200:
            check_results['status'] = 'error'
            check_results['message'] = f"Failed to retrieve users. Status code: {users_response.status_code}"
            return check_results

        users = users_response.json()

        # Step 3: Build a mapping from person names to users
        users_by_person_name = {}
        for user in users:
            is_person = user.get('isPerson')
            if is_person:
                # isPerson is in format "Last name, First name"
                users_by_person_name[is_person] = user

        # Step 4: Find persons who don't have a matching user
        persons_without_users = []
        for person in persons:
            person_uuid = person.get('id')
            given_name = person.get('givenName')
            family_name = person.get('familyName')

            # Skip persons without name information
            if not given_name or not family_name:
                continue

            # Format the person name as it would appear in isPerson field
            person_name_format = f"{family_name}, {given_name}"

            # Check if this person has a user
            if person_name_format not in users_by_person_name:
                # Get posts this person holds
                holds_posts = person.get('holdsPost', [])

                # Only report persons with posts as issues
                if holds_posts:
                    persons_without_users.append({
                        'person_uuid': person_uuid,
                        'given_name': given_name,
                        'family_name': family_name,
                        'posts_count': len(holds_posts)
                    })

        # Step 5: Report persons without users as issues
        if persons_without_users:
            for person in persons_without_users:
                issue = {
                    'type': 'person_without_user',
                    'person_uuid': person['person_uuid'],
                    'given_name': person['given_name'],
                    'family_name': person['family_name'],
                    'posts_count': person['posts_count'],
                    'message': f"Person {person['given_name']} {person['family_name']} has {person['posts_count']} posts but no associated user",
                    'remediation_attempted': False,
                    'remediation_reason': "Automatic user creation requires an email address"
                }
                check_results['issues'].append(issue)

            check_results['status'] = 'warning'
            check_results['message'] = f"Found {len(persons_without_users)} persons with posts but no associated user."
        else:
            check_results['status'] = 'success'
            check_results['message'] = "All persons with posts have associated users."

    except Exception as e:
        # Capture error information
        error_message = str(e)
        error_traceback = traceback.format_exc()
        logging.error(f"Exception occurred during check: {error_message}")
        logging.error(f"Traceback: {error_traceback}")

        # Update the check_results with error status
        check_results['status'] = 'error'
        check_results['message'] = f"Person-user association check failed. Error: {error_message}."
        check_results['error'] = error_traceback

    return check_results
