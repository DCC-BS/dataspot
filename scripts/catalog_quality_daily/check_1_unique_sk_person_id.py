import logging
from typing import Dict, List

import config
from src.clients.base_client import BaseDataspotClient


def check_1_unique_sk_person_id(dataspot_client: BaseDataspotClient) -> Dict[str, any]:
    """
    Check #1: Eindeutigkeitsprüfung
    
    This check verifies that all persons have unique sk_person_id values.
    
    Specifically:
    - For all persons with sk_person_id set, it checks:
        - The sk_person_id is unique (no duplicates)
    
    If not:
    - If duplicates are found, they are reported
    - All problems are documented in the report
    
    Args:
        dataspot_client: Base client for database operations
        
    Returns:
        dict: Check results including status, issues, and any errors
    """
    logging.info("Starting Check #1: Eindeutigkeitsprüfung...")
    
    result = {
        'status': 'success',
        'message': 'All persons have unique sk_person_id values.',
        'issues': [],
        'error': None
    }
    
    try:
        # Get all persons with sk_person_id set
        persons_with_skid = get_persons_with_skid(dataspot_client)
        
        if not persons_with_skid:
            result['message'] = 'No persons with sk_person_id found.'
            return result
            
        logging.info(f"Found {len(persons_with_skid)} persons with sk_person_id to verify")
        
        # Check for duplicates
        logging.info(f"Processing {len(persons_with_skid)} persons for duplicate sk_person_id values...")
        duplicates = find_duplicate_sk_person_ids(persons_with_skid)
        
        if duplicates:
            result['message'] = f"Check #1: Found {len(duplicates)} duplicate sk_person_id values"
            
            for duplicate in duplicates:
                result['issues'].append({
                    'type': 'duplicate_sk_person_id',
                    'sk_person_id': duplicate['sk_person_id'],
                    'person_uuids': duplicate['person_uuids'],
                    'person_names': duplicate['person_names'],
                    'message': f"Duplicate sk_person_id '{duplicate['sk_person_id']}' found for {len(duplicate['person_uuids'])} persons. URLs: {', '.join([f'{config.base_url}/web/{config.database_name}/persons/{uuid}' for uuid in duplicate['person_uuids']])}",
                    'remediation_attempted': False,
                    'remediation_success': False
                })
        else:
            result['message'] = 'Check #1: All sk_person_id values are unique'
    
        # Update final status and message
        if result['issues']:
            issue_count = len(result['issues'])
            # Since this check doesn't remediate, all issues are actual issues
            actual_issues = issue_count
            
            result['status'] = 'warning'
            result['message'] = f"Check #1: Found {actual_issues} duplicate sk_person_id values requiring attention"
    
    except Exception as e:
        result['status'] = 'error'
        result['error'] = str(e)
        result['message'] = f"Error in Check #1 (Eindeutigkeitsprüfung): {str(e)}"
        logging.error(f"Error in Check #1 (Eindeutigkeitsprüfung): {str(e)}", exc_info=True)
    
    return result


def get_persons_with_skid(dataspot_client: BaseDataspotClient) -> List[Dict[str, any]]:
    """
    Get all persons with sk_person_id set.
    
    Args:
        dataspot_client: Database client
        
    Returns:
        list: Persons with sk_person_id
    """
    query = """
    SELECT 
        p.id AS person_uuid,
        p.given_name,
        p.family_name,
        cp.value AS sk_person_id
    FROM 
        person_view p
    JOIN
        customproperties_view cp ON p.id = cp.resource_id AND cp.name = 'sk_person_id'
    WHERE 
        cp.value IS NOT NULL
    ORDER BY
        cp.value, p.family_name, p.given_name
    """
    
    return dataspot_client.execute_query_api(sql_query=query)


def find_duplicate_sk_person_ids(persons_with_skid: List[Dict[str, any]]) -> List[Dict[str, any]]:
    """
    Find duplicate sk_person_id values.
    
    Args:
        persons_with_skid: List of persons with sk_person_id
        
    Returns:
        list: List of duplicate entries with details
    """
    # Group persons by sk_person_id
    skid_groups = {}
    
    for person in persons_with_skid:
        sk_person_id = person.get('sk_person_id', '').strip('"')
        if sk_person_id:
            if sk_person_id not in skid_groups:
                skid_groups[sk_person_id] = []
            skid_groups[sk_person_id].append(person)
    
    # Find groups with more than one person
    duplicates = []
    
    for sk_person_id, persons in skid_groups.items():
        if len(persons) > 1:
            person_uuids = [p['person_uuid'] for p in persons]
            person_names = [f"{p['given_name']} {p['family_name']}" for p in persons]
            
            logging.info(f"Found duplicate sk_person_id '{sk_person_id}' for {len(persons)} persons: {', '.join(person_names)}")
            
            duplicates.append({
                'sk_person_id': sk_person_id,
                'person_uuids': person_uuids,
                'person_names': person_names,
                'count': len(persons)
            })
    
    return duplicates
