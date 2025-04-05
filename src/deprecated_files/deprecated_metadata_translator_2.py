from typing import Dict, Any
import pytz
from dateutil import parser

from src.dataspot_client import DataspotClient
from src.dataspot_dataset import OGDDataset


def ods_to_dataspot(ods_metadata: Dict[str, Any], ods_dataset_id: str) -> OGDDataset:
    """
    Translates metadata from OpenDataSoft (ODS) format to Dataspot format.
    
    This function takes the metadata obtained from the ODS API and transforms it into
    a OGDDataset object. It maps fields from the ODS metadata structure to their 
    corresponding Dataspot metadata fields.
    
    Args:
        ods_metadata (Dict[str, Any]): The metadata dictionary obtained from ODS API.
            Expected to contain fields like dataset name, description, keywords, etc.
    
    Returns:
        Dict[str, Any]: A dictionary containing the metadata in Dataspot format.
    """
    # Get the dataset timezone if available, otherwise default to UTC
    dataset_timezone = None
    if 'default' in ods_metadata and 'timezone' in ods_metadata['default']:
        dataset_timezone = get_field_value(ods_metadata['default']['timezone'])
    
    ogd_dataset = OGDDataset(
        name=get_field_value(ods_metadata['default']['title']),
        beschreibung=get_field_value(ods_metadata['default'].get('description', {})),
        schluesselwoerter=get_field_value(ods_metadata['default'].get('keyword', {})),
        synonyme=None,
        aktualisierungszyklus=get_field_value(
            ods_metadata.get('dcat', {}).get('accrualperiodicity', {'value': None})
        ),
        identifikation=f"ogd_{ods_dataset_id}", # TODO: Remove this field.
        publikationsdatum=iso_8601_to_unix_timestamp(
            get_field_value(ods_metadata.get('dcat', {}).get('issued')), 
            dataset_timezone
        ),
        datenportal_identifikation=ods_dataset_id,
        tags=get_field_value(ods_metadata.get('custom', {}).get('tags', {}))
    )
    return ogd_dataset

def iso_8601_to_unix_timestamp(datetime_str: str, dataset_timezone: str = None) -> int:
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
        int: Unix timestamp in milliseconds (UTC)
        
    Examples:
        >>> iso_8601_to_unix_timestamp("2025-03-07T00:00:00Z")  # UTC timezone is in the string
        1741305600000
        >>> iso_8601_to_unix_timestamp("2025-03-07T00:00:00", "Europe/Zurich")  # Use dataset timezone
        1741302000000  # (accounts for timezone difference)
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
        print(f"Error parsing datetime '{datetime_str}': {e}")
        return None

def get_field_value(field: Dict[str, Any]) -> Any:
    """
    Extracts the value for a metadata field based on the 'override_remote_value' flag.
    
    If 'override_remote_value' exists and is True, the local 'value' is returned.
    If 'override_remote_value' exists and is False, the 'remote_value' is returned.
    If 'override_remote_value' does not exist, 'value' is returned directly.
    If field is an empty dict, None is returned.
    """
    if not field:
        return None
        
    if 'override_remote_value' in field:
        return field['value'] if field['override_remote_value'] else field.get('remote_value', None)
    return field['value']
