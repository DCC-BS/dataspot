import logging
import time
from typing import Dict, List, Tuple, Any, Optional

import config
import requests
from src.common import requests_patch
from src.clients.base_client import BaseDataspotClient
from src.dataspot_auth import DataspotAuth
from src.staatskalender_cache import StaatskalenderCache

# Global cache for person data (Dataspot database caches, not Staatskalender)
_contact_details_cache = None

def check_6_person_contact_details(dataspot_client: BaseDataspotClient, staatskalender_cache: StaatskalenderCache) -> Dict[str, any]:
    """
    Check #6: Kontaktdetails bei Personen

    Verifies that all persons with a set sk_person_id have the correct contact
    details from Staatskalender.

    Specifically checks (see defined_checks.md):
    - Phone number in Staatskalender matches Dataspot
    - E-mail address in Staatskalender matches Dataspot
    - Teams link contains the correct e-mail address
    - Contact website is correctly populated

    If differences are found:
    - Details in Dataspot are adjusted to match Staatskalender
    - All changes are documented for reporting

    Args:
        dataspot_client: Base client for database operations
        staatskalender_cache: Cache instance for Staatskalender API data

    Returns:
        dict: Check results including status, issues, and any errors.
    """
    logging.info("Starting Check #6: Kontaktdetails bei Personen...")

    # Always refresh contact details cache to avoid stale data when the external state was reset
    global _contact_details_cache
    _contact_details_cache = None

    result = {
        'status': 'success',
        'message': 'All persons that are linked to the Staatskalender have consistent contact details with Staatskalender info.',
        'issues': [],
        'error': None,
    }

    try:
        # Load all persons with sk_person_id and their current contact details
        persons_with_contact_details = get_persons_with_contact_details(dataspot_client)
        
        if not persons_with_contact_details:
            result['message'] = 'No persons with sk_person_id found.'
            return result
        
        logging.info(f"Found {len(persons_with_contact_details)} persons with sk_person_id to verify")
        
        # Process each person
        total_persons = len(persons_with_contact_details)
        for current_idx, person in enumerate(persons_with_contact_details, 1):
            person_uuid = person['person_uuid']
            sk_person_id = person['sk_person_id'].strip('"')
            given_name = person.get('given_name', '')
            family_name = person.get('family_name', '')
            person_name = f"{given_name} {family_name}".strip()

            logging.info(f"[{current_idx}/{total_persons}] {person_name}:")
            
            # Add delay to prevent overwhelming the API
            time.sleep(1)
            
            # Get person data from Staatskalender cache
            try:
                person_data = staatskalender_cache.get_person_by_id(sk_person_id)
                sk_email = person_data.get('email')
                sk_phone = person_data.get('phone')
            except Exception as e:
                result['issues'].append({
                    'type': 'staatskalender_data_retrieval_failed',
                    'person_uuid': person_uuid,
                    'given_name': given_name,
                    'family_name': family_name,
                    'sk_person_id': sk_person_id,
                    'message': f"Could not retrieve person data from Staatskalender: {str(e)}",
                    'remediation_attempted': False,
                    'remediation_success': False
                })
                logging.info(f' - Could not retrieve person data from Staatskalender for {sk_person_id}: {str(e)}')
                continue
            
            # Build target customProperties
            target_custom_properties = build_target_custom_properties(
                sk_person_id=sk_person_id,
                sk_email=sk_email,
                sk_phone=sk_phone,
                given_name=given_name,
                family_name=family_name
            )
            
            # Compare and update if needed
            update_needed, differences = compare_and_determine_updates(
                current_custom_properties={
                    'email_custom_property': person.get('email_custom_property'),
                    'phone': person.get('phone'),
                    'state_calendar_website': person.get('state_calendar_website'),
                    'teams': person.get('teams')
                },
                target_custom_properties=target_custom_properties
            )
            
            if update_needed:
                try:
                    update_person_contact_details(
                        dataspot_client=dataspot_client,
                        person_uuid=person_uuid,
                        custom_properties=target_custom_properties
                    )
                    
                    result['issues'].append({
                        'type': 'contact_details_updated',
                        'person_uuid': person_uuid,
                        'given_name': given_name,
                        'family_name': family_name,
                        'sk_person_id': sk_person_id,
                        'differences': differences,
                        'message': f"Updated contact details for {person_name}",
                        'remediation_attempted': True,
                        'remediation_success': True
                    })
                    logging.info(f' - Updated contact details for {person_name} (Link: {config.base_url}/web/{config.database_name}/persons/{person_uuid})')
                except Exception as e:
                    result['issues'].append({
                        'type': 'contact_details_update_failed',
                        'person_uuid': person_uuid,
                        'given_name': given_name,
                        'family_name': family_name,
                        'sk_person_id': sk_person_id,
                        'differences': differences,
                        'message': f"Failed to update contact details for {person_name}: {str(e)}",
                        'remediation_attempted': True,
                        'remediation_success': False
                    })
                    logging.error(f' - Failed to update contact details for {person_name}: {str(e)}')
            else:
                logging.info(f' - Contact details already correct for {person_name}')
        
        # Update final status and message
        if result['issues']:
            issue_count = len(result['issues'])
            remediated_count = sum(1 for issue in result['issues'] 
                                  if issue.get('remediation_attempted', False) 
                                  and issue.get('remediation_success', False))
            actual_issues = issue_count - remediated_count
            
            if actual_issues > 0:
                result['status'] = 'warning'
                result['message'] = f"Check #6: Found {issue_count} issue(s) ({remediated_count} automatically fixed, {actual_issues} requiring attention)"
            else:
                result['message'] = f"Check #6: Fixed {remediated_count} issue(s), all contact details are correctly synchronized"
    
    except Exception as e:
        result['status'] = 'error'
        result['error'] = str(e)
        result['message'] = f"Error in Check #6 (Kontaktdetails bei Personen): {str(e)}"
        logging.error(f"Error in Check #6 (Kontaktdetails bei Personen): {str(e)}", exc_info=True)
    
    return result


def get_persons_with_contact_details(dataspot_client: BaseDataspotClient) -> List[Dict[str, Any]]:
    """
    Get all persons with sk_person_id and their current contact custom properties.
    
    Args:
        dataspot_client: Database client
        
    Returns:
        List of dicts with person info and current contact custom properties
    """
    global _contact_details_cache
    
    if _contact_details_cache is not None:
        return _contact_details_cache
    
    logging.debug("Loading persons with contact details cache...")
    
    query = """
    SELECT 
        p.id AS person_uuid,
        p.given_name,
        p.family_name,
        p.additional_name,
        cp_sk.value AS sk_person_id,
        cp_email.value AS email_custom_property,
        cp_phone.value AS phone,
        cp_website.value AS state_calendar_website,
        cp_teams.value AS teams
    FROM 
        person_view p
    JOIN
        customproperties_view cp_sk ON p.id = cp_sk.resource_id AND cp_sk.name = 'sk_person_id'
    LEFT JOIN
        customproperties_view cp_email ON p.id = cp_email.resource_id AND cp_email.name = 'email_custom_property'
    LEFT JOIN
        customproperties_view cp_phone ON p.id = cp_phone.resource_id AND cp_phone.name = 'phone'
    LEFT JOIN
        customproperties_view cp_website ON p.id = cp_website.resource_id AND cp_website.name = 'state_calendar_website'
    LEFT JOIN
        customproperties_view cp_teams ON p.id = cp_teams.resource_id AND cp_teams.name = 'teams'
    WHERE 
        cp_sk.value IS NOT NULL
    ORDER BY
        p.family_name, p.given_name
    """
    
    results = dataspot_client.execute_query_api(sql_query=query)
    _contact_details_cache = []
    
    for result in results:
        # Strip quotes from string values
        given_name = (result.get('given_name') or '').strip('"').strip()
        additional_name = (result.get('additional_name') or '').strip('"').strip() if result.get('additional_name') else ''
        family_name = (result.get('family_name') or '').strip('"').strip()

        # given_name and family_name are mandatory; abort loudly if missing
        if not given_name or not family_name:
            raise ValueError("Person record missing mandatory given_name or family_name")

        person_data = {
            'person_uuid': result['person_uuid'],
            'given_name': given_name,
            'family_name': family_name,
            'additional_name': additional_name if additional_name else None,
            'sk_person_id': result['sk_person_id'],
            'email_custom_property': result.get('email_custom_property', '').strip('"') if result.get('email_custom_property') else None,
            'phone': result.get('phone', '').strip('"') if result.get('phone') else None,
            'state_calendar_website': result.get('state_calendar_website', '').strip('"') if result.get('state_calendar_website') else None,
            'teams': result.get('teams', '').strip('"') if result.get('teams') else None
        }
        _contact_details_cache.append(person_data)
    
    logging.debug(f"Persons with contact details cache loaded with {len(_contact_details_cache)} entries")
    return _contact_details_cache


def email_is_valid_teams_email_address(email_address: str) -> bool:
    """
    Checks whether a given email address is available in Entra ID

    Args:
        email_address: Email from Staatskalender

    Returns:
        bool: True if email is valid, False otherwise.
    """

    global dataspot_auth

    url = f"https://graph.microsoft.com/v1.0/users?$filter=mail eq '{email_address}'"
    response = requests.get(url=url, headers=dataspot_auth.get_headers())
    assert False
    # TODO: Implement this once the Entra APP exists and has sufficient authorization
    return True

def build_target_custom_properties(sk_person_id: str, sk_email: Optional[str], sk_phone: Optional[str], 
                                    given_name: str, family_name: str) -> Dict[str, Optional[str]]:
    """
    Build the target customProperties payload based on Staatskalender data.
    
    Args:
        sk_person_id: Staatskalender person ID
        sk_email: Email from Staatskalender
        sk_phone: Phone from Staatskalender
        given_name: Person's given name
        family_name: Person's family name
        
    Returns:
        dict: Target customProperties to set
    """
    custom_properties: Dict[str, Optional[str]] = {}
    
    # email_custom_property
    if sk_email:
        custom_properties['email_custom_property'] = sk_email
    else:
        custom_properties['email_custom_property'] = None
    
    # phone - format as tel link
    if sk_phone:
        # Format phone number for tel link (remove spaces, keep + and digits)
        phone_clean = ''.join(c for c in sk_phone if c.isdigit() or c == '+')
        # Create display format (show original with some masking if needed, or just show original)
        # For now, use the original phone number in the display
        custom_properties['phone'] = f"[{sk_phone}](tel:{phone_clean})"
    else:
        custom_properties['phone'] = None
    
    # state_calendar_website
    custom_properties['state_calendar_website'] = f"[Kontaktseite im Staatskalender öffnen](https://staatskalender.bs.ch/person/{sk_person_id})"
    
    # teams - only if valid email exists
    if email_is_valid_teams_email_address(sk_email):
        custom_properties['teams'] = f"[Teams-Chat mit {given_name} {family_name} öffnen](msteams://teams.microsoft.com/l/chat/0/0?users={sk_email})"
    else:
        custom_properties['teams'] = None
    
    return custom_properties


def compare_and_determine_updates(current_custom_properties: Dict[str, Optional[str]], 
                                  target_custom_properties: Dict[str, str]) -> Tuple[bool, Dict[str, Dict[str, str]]]:
    """
    Compare current and target custom properties to determine if update is needed.
    
    Args:
        current_custom_properties: Current custom properties in Dataspot
        target_custom_properties: Target custom properties from Staatskalender
        
    Returns:
        Tuple of (update_needed: bool, differences: dict)
    """
    differences = {}
    update_needed = False
    
    for key in ['email_custom_property', 'phone', 'state_calendar_website', 'teams']:
        current_value = current_custom_properties.get(key)
        target_value = target_custom_properties.get(key)
        
        # Normalize None and empty string
        current_normalized = current_value if current_value else None
        target_normalized = target_value if target_value else None
        
        if current_normalized != target_normalized:
            differences[key] = {
                'current': current_normalized or '(not set)',
                'target': target_normalized or '(not set)'
            }
            update_needed = True
    
    return update_needed, differences


def update_person_contact_details(dataspot_client: BaseDataspotClient, person_uuid: str, 
                                  custom_properties: Dict[str, str]) -> None:
    """
    Update person contact details via REST API.
    
    Args:
        dataspot_client: Database client
        person_uuid: Person UUID to update
        custom_properties: Custom properties to set
        
    Returns:
        None
    """
    person_update = {
        "_type": "Person",
        "customProperties": custom_properties
    }
    
    update_url = f"{dataspot_client.base_url}/rest/{dataspot_client.database_name}/persons/{person_uuid}"
    
    response = requests_patch(
        url=update_url,
        json=person_update,
        headers=dataspot_client.auth.get_headers()
    )
    
    response.raise_for_status()
    
    # Reset cache since person data was modified
    global _contact_details_cache
    _contact_details_cache = None

if __name__=='__main__':
    print("Hello, world!")

    dataspot_auth = DataspotAuth()
    token_url = dataspot_auth.token_url
    token_data = {
        'grant_type': 'client_credentials',
        'client_id': dataspot_auth.client_id,
        'client_secret': dataspot_auth.client_secret,
        'scope': 'https://graph.microsoft.com/.default'
    }
    token_response = requests.post(url=token_url, data=token_data)
    access_token = token_response.json()['access_token']

    headers = {'Authorization': f'Bearer {access_token}'}
    email_to_check = 'renato.farruggio@bs.ch'
    url = f"https://graph.microsoft.com/v1.0/users?$filter=mail eq '{email_to_check}' or userPrincipalName eq '{email_to_check}'&$select=id,displayName,mail"
    response = requests.get(url, headers=headers)  # Currently returns "insufficient privileges" error
    # TODO: Check privileges at https://portal.azure.com/#view/Microsoft_AAD_RegisteredApps/ApplicationMenuBlade/~/CallAnAPI/appId/82efb63b-e1a6-49a8-bc67-23a5b58caf74/isMSAApp~/false
    # Current Perplexity.ai chat: https://www.perplexity.ai/search/entra-id-check-if-mail-exists-kSwm.sjPSQCF7tSuMJY6UA#1
    # TODO: Check this person mail (not valid teams-mail): https://datenkatalog.bs.ch/web/prod/persons/a010e53b-0ba2-462e-b344-b33b4a0cbf8e
    # TODO: Check renato person mail: renato.farruggio@bs.ch

    #email_is_valid_teams_email_address('renato.farruggio@bs.ch')
    pass