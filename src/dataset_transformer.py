from typing import Dict, Any, Optional, List
import logging
import pytz
from dateutil import parser

from src.dataspot_dataset import OGDDataset


# Map of ODS geographic reference codes to human-readable locations
# TODO (Renato): Add these codes to the RDM manually, and then use them instead of this map
GEOGRAPHIC_REFERENCE_MAP = {
    "world_ch": "Schweiz",
    "ch_80_2703": "Riehen",
    "ch_80_2702": "Bettingen",
    "ch_80_2701": "Basel",
    "ch_80_2765": "Binningen",
    "ch_80_2762": "Allschwil",
    "ch_80_2766": "Birsfelden",
    "ch_80_2767": "Bottmingen",
    "ch_80_2774": "Schönenbuch",
    "ch_40_12": "Basel-Stadt",
    "ch_40_13": "Basel-Landschaft",
    "ch_80_2761": "Aesch (BL)",
    "ch_80_2763": "Arlesheim",
    "ch_80_2822": "Augst",
    "ch_80_2473": "Dornach",
    "ch_80_4161": "Eiken",
    "ch_80_2768": "Ettingen",
    "ch_80_2824": "Frenkendorf",
    "ch_80_4163": "Frick",
    "ch_80_2825": "Füllinsdorf",
    "ch_80_4165": "Gipf-Oberfrick",
    "ch_80_4252": "Kaiseraugst",
    "ch_80_2828": "Lausen",
    "ch_80_2829": "Liestal",
    "ch_80_4254": "Möhlin",
    "ch_80_2769": "Münchenstein",
    "ch_80_2770": "Muttenz",
    "ch_80_2771": "Oberwil (BL)",
    "ch_80_4175": "Oeschgen",
    "ch_80_2772": "Pfeffingen",
    "ch_80_2831": "Pratteln",
    "ch_80_2773": "Reinach (BL)",
    "ch_80_4258": "Rheinfelden",
    "ch_80_2775": "Therwil",
    "ch_80_4261": "Wallbach"
    # Add more mappings as needed
}

# TODO (Renato): URGENT! IMPORTANT! Lizenz und Rechte referenzdatenmodell updaten. Dann hier anpassen.
# Map of ODS rights values to human-readable descriptions
NUTZUNGSRECHTE_MAP = {
    "NonCommercialAllowed-CommercialAllowed-ReferenceRequired": "Creative Commons Attribution (CC BY)",
    # Add more license mappings as needed
}

# Map of ODS license_id values to human-readable license URLs
LIZENZ_MAP = {
    "4bj8ceb": ["https://creativecommons.org/publicdomain/zero/1.0/"],    # CC0 1.0
    "cc_by": ["https://creativecommons.org/licenses/by/3.0/ch/"],         # CC BY 3.0 CH
    "5sylls5": ["https://creativecommons.org/licenses/by/4.0/ch/"],       # CC BY 4.0 CH
    "t2kf10u": ["https://creativecommons.org/licenses/by/3.0/ch/", "https://opendatacommons.org/licenses/odbl/1-0/"],       # CC BY 3.0 CH + OpenStreetMap
    "353v4r": ["https://creativecommons.org/licenses/by/4.0/ch/", "https://opendatacommons.org/licenses/odbl/1-0/"],        # CC BY 4.0 CH + OpenStreetMap
    "vzo5u7j": ["https://www.gnu.org/licenses/gpl-3.0"],                 # GNU General Public License 3
    "r617wgj": ["https://www.bs.ch/bvd/grundbuch-und-vermessungsamt/geo/anwendungen/agb"], # Nutzungsbedingungen für Geo…
    "hmpvfpp": ["https://opendata.swiss/de/terms-of-use/"], # Freie Nutzung. Kommerzielle …  # TODO: CC_ASK
    "ce0mv1b": ["https://opendata.swiss/de/terms-of-use/"], # Freue Nutzung. Quellenangab... # TODO: CC_BY_ASK
}

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
    if 'default' in ods_metadata and 'geographic_reference' in ods_metadata['default']:
        geo_refs = get_field_value(ods_metadata['default']['geographic_reference'])
        if geo_refs and isinstance(geo_refs, list) and len(geo_refs) > 0:
            # Check if all codes are in the map
            all_codes_in_map = True
            unknown_codes = []
            for geo_code in geo_refs:
                if geo_code is not None and geo_code not in GEOGRAPHIC_REFERENCE_MAP:
                    all_codes_in_map = False
                    unknown_codes.append(geo_code)
            
            if unknown_codes:
                # Only throw an error for unknown codes (not for None)
                raise ValueError(f"Unknown geographic reference code(s): {unknown_codes}")
            
            # If all codes are in the map, add all of them
            if all_codes_in_map:
                # Create a list of geo dimensions for valid codes, filter out None values
                geo_dimensions = [GEOGRAPHIC_REFERENCE_MAP[geo_code] for geo_code in geo_refs if geo_code is not None]
                
                # Join the values into a single string with comma and space separator
                geographical_dimension = ", ".join(geo_dimensions) if geo_dimensions else None
                
                if len(geo_refs) > 1:
                    logging.info(f"Multiple geographic references found in ODS metadata: {geo_refs}. Joined as: {geographical_dimension}")
    
    lizenz = None
    nutzungsrechte = None
    if False:   # TODO (Renato): Add this once it's settled how we want it.
        # Extract license/rights information
        
        # Get Nutzungsrechte from dcat_ap_ch.rights
        if 'dcat_ap_ch' in ods_metadata and 'rights' in ods_metadata['dcat_ap_ch']:
            rechte_wert = get_field_value(ods_metadata['dcat_ap_ch']['rights'])
            if rechte_wert and rechte_wert not in NUTZUNGSRECHTE_MAP:
                raise ValueError(f"Unknown rights value: {rechte_wert}")
            elif rechte_wert:
                nutzungsrechte = rechte_wert
                logging.info(f"Found rights value: {nutzungsrechte}")
        
        # Get Lizenz from internal.license_id
        if 'internal' in ods_metadata and 'license_id' in ods_metadata['internal']:
            license_id = get_field_value(ods_metadata['internal']['license_id'])
            if license_id and license_id not in LIZENZ_MAP:
                raise ValueError(f"Unknown license ID: {license_id}")
            elif license_id:
                lizenz = LIZENZ_MAP[license_id]
                logging.info(f"Mapped license ID '{license_id}' to '{lizenz}'")
    
    # TODO (Renato): Map temporal coverage information (example: "1939-08-01/2025-03-31" or "2024-02-10/2032-08-08")
    
    # TODO (Renato): Map dcat.creator to appropriate field (example: "Erziehungsdepartement" or "Statistisches Amt")
    # Note: Will need to add this field to dataspot_dataset.py annotations YAML
    
    # TODO (Renato): Map default.publisher to appropriate field (example: "Generalsekretariat" or "Statistisches Amt")
    # Note: Will need to add this field to dataspot_dataset.py annotations YAML
    
    # TODO (Renato): Map default.references to appropriate field (example: "https://statistik.bs.ch/unterthema/9#Preise")
    
    # TODO (Renato): Consider if it makes sense to import creation date (dcat.created) and modification date (default.modified)
    
    # Create the OGDDataset with mapped fields
    ogd_dataset = OGDDataset(
        # Basic information
        name=get_field_value(ods_metadata['default']['title']),
        beschreibung=get_field_value(ods_metadata['default'].get('description', {})),
        
        # Keywords and categorization
        schluesselwoerter=get_field_value(ods_metadata['default'].get('keyword', {})),
        
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
        
        # License information
        lizenz=lizenz,
        
        nutzungsrechte=nutzungsrechte,
        
        # Identifiers
        datenportal_identifikation=ods_dataset_id,
        
        # Custom properties
        tags=get_field_value(ods_metadata.get('custom', {}).get('tags', {}))
    )
    
    logging.debug(f"Transformed ODS dataset '{ods_dataset_id}' to DNK format")
    return ogd_dataset


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