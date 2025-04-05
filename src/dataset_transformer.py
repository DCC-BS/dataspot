from typing import Dict, Any, Optional, List
import logging
import pytz
from dateutil import parser

from src.dataspot_dataset import OGDDataset


def transform_ods_to_dnk(ods_metadata: Dict[str, Any], ods_dataset_id: str) -> OGDDataset:
    """
    Transforms metadata from OpenDataSoft (ODS) format to Dataspot DNK format.
    
    This function takes the metadata obtained from the ODS API and transforms it into
    a OGDDataset object for use in the Dataspot DNK (Datennutzungskatalog).
    It maps fields from the ODS metadata structure to their corresponding Dataspot fields.
    
    Args:
        ods_metadata (Dict[str, Any]): The metadata dictionary obtained from ODS API.
            Expected to contain fields like dataset name, description, keywords, etc.
        ods_dataset_id (str): The ODS dataset ID, used for identification.
    
    Returns:
        OGDDataset: A dataset object containing the metadata in Dataspot format.
    """
    # Get the dataset timezone if available, otherwise default to UTC
    dataset_timezone = None
    if 'default' in ods_metadata and 'timezone' in ods_metadata['default']:
        dataset_timezone = get_field_value(ods_metadata['default']['timezone'])
    
    # Extract geographical/spatial information if available
    geographical_dimension = None
    if 'dcat' in ods_metadata and 'spatial' in ods_metadata['dcat']:
        geographical_dimension = get_field_value(ods_metadata['dcat']['spatial'])
    
    # Create the OGDDataset with mapped fields
    ogd_dataset = OGDDataset(
        # Basic information
        name=get_field_value(ods_metadata['default']['title']),
        beschreibung=get_field_value(ods_metadata['default'].get('description', {})),
        kurzbeschreibung=extract_short_description(ods_metadata),
        
        # Keywords and categorization
        schluesselwoerter=get_field_value(ods_metadata['default'].get('keyword', {})),
        synonyme=extract_synonyms(ods_metadata),
        
        # Time and update information
        aktualisierungszyklus=get_field_value(
            ods_metadata.get('dcat', {}).get('accrualperiodicity', {'value': None})
        ),
        publikationsdatum=iso_8601_to_unix_timestamp(
            get_field_value(ods_metadata.get('dcat', {}).get('issued')), 
            dataset_timezone
        ),
        
        # Geographic information
        geographische_dimension=geographical_dimension,
        
        # Identifiers
        datenportal_identifikation=ods_dataset_id,
        
        # Custom properties
        tags=get_field_value(ods_metadata.get('custom', {}).get('tags', {}))
    )
    
    logging.debug(f"Transformed ODS dataset '{ods_dataset_id}' to DNK format")
    return ogd_dataset


def extract_short_description(ods_metadata: Dict[str, Any]) -> Optional[str]:
    """
    Extract a short description from the ODS metadata.
    
    Args:
        ods_metadata: The ODS metadata dictionary
        
    Returns:
        Optional[str]: A short description if available, otherwise None
    """
    # TODO (Renato): I don't think we need this method as there is no short description in the ODS metadata I THINK. Please check and remove.
    return None
    # Try to get a short description from various places in the metadata
    if 'default' in ods_metadata:
        # Check if there's a field specifically for short description
        if 'short_description' in ods_metadata['default']:
            return get_field_value(ods_metadata['default']['short_description'])
        
        # If there's a description, take the first sentence or first 100 chars
        if 'description' in ods_metadata['default']:
            desc = get_field_value(ods_metadata['default']['description'])
            if desc:
                # Return first sentence (up to .) or first 100 chars
                first_sentence = desc.split('. ')[0]
                if len(first_sentence) > 100:
                    return first_sentence[:97] + '...'
                return first_sentence
    
    return None


def extract_synonyms(ods_metadata: Dict[str, Any]) -> Optional[List[str]]:
    """
    Extract synonyms from the ODS metadata if available.
    
    Args:
        ods_metadata: The ODS metadata dictionary
        
    Returns:
        Optional[List[str]]: List of synonyms if available, otherwise None
    """
    # Check various places where synonyms might be stored
    if 'default' in ods_metadata and 'synonyms' in ods_metadata['default']:
        return get_field_value(ods_metadata['default']['synonyms'])
    
    # Could also extract from 'alternate_title' if available
    if 'default' in ods_metadata and 'alternate_title' in ods_metadata['default']:
        alt_title = get_field_value(ods_metadata['default']['alternate_title'])
        if alt_title:
            if isinstance(alt_title, list):
                return alt_title
            return [alt_title]
    
    return None


def iso_8601_to_unix_timestamp(datetime_str: str, dataset_timezone: str = None) -> Optional[int]:
    """
    Converts an ISO 8601 formatted datetime string to a Unix timestamp in milliseconds.
    
    This function handles different ISO 8601 formats and timezone information.
    If a timezone is specified in the datetime string, it will be respected.
    If no timezone is in the string but a dataset_timezone is provided, that will be used.
    Otherwise, UTC is assumed as the fallback.
    
    Args:
        datetime_str (str): ISO 8601 formatted datetime string (e.g., "2025-03-07T00:00:00Z")
        dataset_timezone (str, optional): The timezone specified in the dataset metadata (e.g., "Europe/Zurich")
        
    Returns:
        Optional[int]: Unix timestamp in milliseconds (UTC), or None if conversion fails
    """
    if not datetime_str:
        return None
    
    # Use dateutil parser to handle various ISO 8601 formats
    try:
        # Parse the datetime string - if it contains timezone info, it will be used
        dt = parser.parse(datetime_str)
        
        # If the datetime has no timezone info but we have a dataset timezone
        if dt.tzinfo is None and dataset_timezone:
            try:
                # Get the timezone object
                tz = pytz.timezone(dataset_timezone)
                # Localize the naive datetime to the dataset timezone
                dt = tz.localize(dt)
            except pytz.exceptions.UnknownTimeZoneError:
                # If timezone is invalid, fall back to UTC
                dt = dt.replace(tzinfo=pytz.UTC)
        elif dt.tzinfo is None:
            # If no timezone info in the string and no dataset timezone, assume UTC
            dt = dt.replace(tzinfo=pytz.UTC)
        
        # Convert to milliseconds, ensuring we're in UTC
        timestamp_ms = int(dt.astimezone(pytz.UTC).timestamp() * 1000)
        return timestamp_ms
    except (ValueError, TypeError) as e:
        # Log the error and return None for invalid datetime strings
        logging.error(f"Error parsing datetime '{datetime_str}': {e}")
        return None


def get_field_value(field: Dict[str, Any] | Any) -> Any:
    """
    Extracts the value for a metadata field based on the 'override_remote_value' flag.
    
    If 'override_remote_value' exists and is True, the local 'value' is returned.
    If 'override_remote_value' exists and is False, the 'remote_value' is returned.
    If 'override_remote_value' does not exist, 'value' is returned directly.
    If field is an empty dict, None is returned.
    
    Args:
        field: A dictionary containing field data or a direct value
        
    Returns:
        The appropriate value from the field
    """
    if field is None:
        return None
    
    # If it's not a dictionary, return it directly    
    if not isinstance(field, dict):
        return field
    
    # If it's an empty dict, return None
    if not field:
        return None
    
    # Handle different field structures
    if 'override_remote_value' in field:
        return field['value'] if field['override_remote_value'] else field.get('remote_value', None)
    
    if 'value' in field:
        return field['value']
    
    # Last resort: return the first value we find
    for key, value in field.items():
        if key not in ('type', 'name', 'label', 'description'):
            return value
    
    return None 