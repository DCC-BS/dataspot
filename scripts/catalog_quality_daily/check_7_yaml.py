import config
import logging
import os
import requests

from src.common import requests_get
from src.clients.base_client import BaseDataspotClient


# List of tuples: (online_name, local_path relative to project root)
# TODO: Once the Umlaute-Bug is fixed:
#  - Remove profile "Buggy-ä-Umlaute" in dataspot
#  - Potentially rename the 3 'ae' back to 'ä'
YAML_PROFILES = [
    ("Mandant", "Annotation YAMLs/annotations__mandant.yaml"),
    ("Datenbankobjekte", "Annotation YAMLs/annotations_datenbankobjekte.yaml"),
    ("Datenprodukte", "Annotation YAMLs/annotations_datenprodukte.yaml"),
    ("Datenqualitaet", "Annotation YAMLs/annotations_datenqualitaet.yaml"),
    ("Datenraeume", "Annotation YAMLs/annotations_datenraeume.yaml"),
    ("Datentypen (fachlich)", "Annotation YAMLs/annotations_datentypen_fachlich.yaml"),
    ("Datentypen (technisch)", "Annotation YAMLs/annotations_datentypen_technisch.yaml"),
    ("Fachdaten", "Annotation YAMLs/annotations_fachdaten.yaml"),
    ("Katalogqualitaet", "Annotation YAMLs/annotations_katalogqualitaet.yaml"),
    ("Kennzahlen", "Annotation YAMLs/annotations_kennzahlen.yaml"),
    ("Prozesse", "Annotation YAMLs/annotations_prozesse.yaml"),
    #("Rechtsgrundlagen", "Annotation YAMLs/annotations_rechtsgrundlagen.yaml"), # TODO: Does not yet exist!
    ("Referenzdaten", "Annotation YAMLs/annotations_referenzdaten.yaml"),
    ("Systeme", "Annotation YAMLs/annotations_systeme.yaml"),
]

def check_7_yaml(dataspot_client: BaseDataspotClient) -> dict:
    """
    Check #7: Annotation YAML vs repo
    
    This check compares annotation YAML files served by the datenkatalog API
    with the versions checked into the repo. The online version is always
    fetched from prod.
    
    Specifically:
    - For each configured YAML profile, it downloads the online version from prod
    - Compares it with the local file in the repo (raw text, normalized line endings)
    - Reports any differences as issues
    
    If differences are found:
    - Issues are reported so the combined email lists which file(s) differ
    - No remediation is attempted
    
    Args:
        dataspot_client: Base client for authentication
        
    Returns:
        dict: Check results including status, issues, and any errors
    """
    logging.debug("Starting Check #7: Annotation YAML vs repo...")
    
    result = {
        'status': 'success',
        'message': 'All annotation YAML files match the repo.',
        'issues': [],
        'error': None
    }
    
    # Get project root (two levels up from this file)
    current_file_path = os.path.abspath(__file__)
    project_root = os.path.dirname(os.path.dirname(os.path.dirname(current_file_path)))
    
    try:
        for online_name, local_path in YAML_PROFILES:
            try:
                # Download online version
                download_url = f"{config.base_url}/api/{config.database_name_prod}/profiles/{online_name}/download"
                response = requests_get(download_url, headers=dataspot_client.auth.get_headers())
                response.raise_for_status()
                online_content = response.text
                
                # Read local file
                local_file_path = os.path.join(project_root, local_path)
                with open(local_file_path, 'r', encoding='utf-8') as f:
                    local_content = f.read()
                
                # Normalize line endings and trailing newline
                online_normalized = normalize_text(online_content)
                local_normalized = normalize_text(local_content)
                
                # Compare
                if online_normalized != local_normalized:
                    issue_message = f"Online profile '{online_name}' differs from local file {local_path}"
                    result['issues'].append({
                        'type': 'yaml_profile_diff',
                        'online_name': online_name,
                        'local_path': local_path,
                        'message': issue_message,
                        'remediation_attempted': False,
                        'remediation_success': False
                    })
                    logging.info(f"{online_name}: difference detected")
                else:
                    logging.info(f"{online_name}: in sync")
                    
            except requests.RequestException as e:
                result['status'] = 'error'
                result['error'] = f"Failed to download {online_name}: {str(e)}"
                result['message'] = f"Error in Check #7: {result['error']}"
                logging.error(f"Failed to download {online_name} from {download_url}: {str(e)}")
                return result
                
            except FileNotFoundError as e:
                result['status'] = 'error'
                result['error'] = f"Local file not found: {local_path}"
                result['message'] = f"Error in Check #7: {result['error']}"
                logging.error(f"Local file not found: {local_path}")
                return result
                
            except Exception as e:
                result['status'] = 'error'
                result['error'] = f"Error comparing {online_name}: {str(e)}"
                result['message'] = f"Error in Check #7: {result['error']}"
                logging.error(f"Error comparing {online_name}: {str(e)}", exc_info=True)
                return result
        
        # Update final status and message
        if result['issues']:
            issue_count = len(result['issues'])
            result['status'] = 'warning'
            result['message'] = f"Check #7: Found {issue_count} YAML file(s) with differences"
            logging.info(f"Check finished: Found {issue_count} YAML file(s) with differences")
        else:
            result['message'] = 'Check #7: All annotation YAML files match the repo'
            logging.info(f"Check finished: All {len(YAML_PROFILES)} YAML file(s) are in sync")
    
    except Exception as e:
        result['status'] = 'error'
        result['error'] = str(e)
        result['message'] = f"Error in Check #7 (Annotation YAML vs repo): {str(e)}"
        logging.error(f"Error in Check #7 (Annotation YAML vs repo): {str(e)}", exc_info=True)
    
    return result


def normalize_text(text: str) -> str:
    """
    Normalize text for comparison by standardizing line endings.
    
    Args:
        text: The text to normalize
        
    Returns:
        str: Normalized text with consistent line endings
    """
    # Convert all line endings to \n
    normalized = text.replace("\r\n", "\n").replace("\r", "\n")
    # Strip trailing newline to avoid false positives
    normalized = normalized.rstrip("\n")
    return normalized
