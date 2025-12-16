import logging
import time
from typing import Dict, List, Tuple, Any, Optional

import config
from src.common import requests_get, requests_patch
from src.clients.base_client import BaseDataspotClient
from src.staatskalender_auth import StaatskalenderAuth

# Global cache for person data
_person_with_sk_id_cache = None
_person_cache = None
_person_email_cache = {}
_contact_details_cache = None
_staatskalender_data_cache = {}

def check_6_person_contact_details(dataspot_client: BaseDataspotClient) -> Dict[str, any]:
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

    Returns:
        dict: Check results including status, issues, and any errors.
    """
    logging.info("Starting Check #6: Kontaktdetails bei Personen...")

    # Always refresh caches to avoid stale data when the external state was reset
    global _person_with_sk_id_cache, _person_cache, _person_email_cache, _contact_details_cache, _staatskalender_data_cache
    _person_with_sk_id_cache = None
    _person_cache = None
    _person_email_cache = {}
    _contact_details_cache = None
    _staatskalender_data_cache = {}

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
        
        # Initialize Staatskalender authentication
        staatskalender_auth = StaatskalenderAuth()
        
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
            
            # Get person data from Staatskalender
            sk_data = get_person_details_from_staatskalender(sk_person_id, staatskalender_auth)
            
            if not sk_data:
                result['issues'].append({
                    'type': 'staatskalender_data_retrieval_failed',
                    'person_uuid': person_uuid,
                    'given_name': given_name,
                    'family_name': family_name,
                    'sk_person_id': sk_person_id,
                    'message': f"Could not retrieve person data from Staatskalender",
                    'remediation_attempted': False,
                    'remediation_success': False
                })
                logging.info(f' - Could not retrieve person data from Staatskalender for {sk_person_id}')
                continue
            
            # Build target customProperties
            target_custom_properties = build_target_custom_properties(
                sk_person_id=sk_person_id,
                sk_email=sk_data.get('email'),
                sk_phone=sk_data.get('phone'),
                given_name=given_name,
                family_name=family_name,
                additional_name=person.get('additional_name')
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


def get_person_details_from_staatskalender(sk_person_id: str, staatskalender_auth: StaatskalenderAuth) -> Dict[str, Any]:
    """
    Retrieve person details from Staatskalender by sk_person_id with caching.
    
    Args:
        sk_person_id: Staatskalender person ID
        staatskalender_auth: Authentication object for Staatskalender API
        
    Returns:
        dict: Person details including email, phone, first_name, last_name or empty dict if error
    """
    global _staatskalender_data_cache
    
    # Check cache first
    if sk_person_id in _staatskalender_data_cache:
        logging.debug(f"Using cached Staatskalender data for person {sk_person_id}")
        return _staatskalender_data_cache[sk_person_id]
    
    logging.debug(f"Retrieving person details from Staatskalender for person with SK ID: {sk_person_id}")
    
    # Add a delay to prevent overwhelming the API
    time.sleep(1)
    
    person_url = f"https://staatskalender.bs.ch/api/people/{sk_person_id}"
    try:
        person_response = requests_get(url=person_url, auth=staatskalender_auth.get_auth())
        
        if person_response.status_code != 200:
            logging.warning(f"Failed to retrieve person data from Staatskalender. Status code: {person_response.status_code}")
            _staatskalender_data_cache[sk_person_id] = {}
            return {}
            
        # Extract person details
        person_data = person_response.json()
        sk_email = None
        sk_phone = None
        sk_first_name = None
        sk_last_name = None
        
        for item in person_data.get('collection', {}).get('items', []):
            for data_item in item.get('data', []):
                field_name = data_item.get('name')
                field_value = data_item.get('value')
                
                if field_name == 'email':
                    sk_email = field_value
                elif field_name == 'phone' or field_name == 'telephone' or field_name == 'phone_number':
                    sk_phone = field_value
                elif field_name == 'first_name':
                    raw_first_name = field_value
                    if raw_first_name:
                        sk_first_name = raw_first_name.strip()
                elif field_name == 'last_name':
                    sk_last_name = field_value
                    if sk_last_name:
                        sk_last_name = sk_last_name.strip()
        
        result = {
            'email': sk_email,
            'phone': sk_phone,
            'first_name': sk_first_name,
            'last_name': sk_last_name
        }
        
        # Cache the result
        _staatskalender_data_cache[sk_person_id] = result
        
        if sk_email:
            logging.debug(f"Found email in Staatskalender: {sk_email}")
        if sk_phone:
            logging.debug(f"Found phone in Staatskalender: {sk_phone}")
            
        return result
        
    except Exception as e:
        logging.error(f"Error retrieving person data from Staatskalender: {str(e)}")
        _staatskalender_data_cache[sk_person_id] = {}
        return {}


def build_target_custom_properties(sk_person_id: str, sk_email: Optional[str], sk_phone: Optional[str], 
                                    given_name: str, family_name: str, additional_name: Optional[str] = None) -> Dict[str, Optional[str]]:
    """
    Build the target customProperties payload based on Staatskalender data.
    
    Args:
        sk_person_id: Staatskalender person ID
        sk_email: Email from Staatskalender
        sk_phone: Phone from Staatskalender
        given_name: Person's given name
        family_name: Person's family name
        additional_name: Person's additional name (optional)
        
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
    
    # teams - only if email exists
    if sk_email:
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
