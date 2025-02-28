from typing import Dict, Any

from src.dataspot_client import DataspotClient
from src.dataspot_dataset import OGDDataset


def ods_to_dataspot(ods_metadata: Dict[str, Any], ods_dataset_id: str, dataspot_client: DataspotClient) -> OGDDataset:
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
    ogd_dataset = OGDDataset(
        name=get_field_value(ods_metadata['default']['title']),
        _PATH=create_path(ods_metadata),
        beschreibung=get_field_value(ods_metadata['default'].get('description', {})),
        schluesselwoerter=get_field_value(ods_metadata['default'].get('keyword', {})),
        synonyme=None,
        aktualisierungszyklus=dataspot_client.rdm_accrualPeriodicity_uri_to_code(
            get_field_value(
                ods_metadata.get('dcat', {}).get('accrualperiodicity', {'value': None})
            )
        ),
        identifikation=f"ogd_{ods_dataset_id}"
    )
    return ogd_dataset

def apply_special_cases(departement: str, dienststelle: str) -> (str, str, str, str):
    """
    Applies special case rules to departement and dienststelle to derive
    the final values for departement, dienststelle, sammlung, and subsammlung.

    The special cases are as follows:
      - If the original dienststelle is in a special set of values, its value is
        moved to sammlung.
      - If dienststelle equals "Parkhäuser Basel-Stadt":
           • subsammlung is set to "Parkhäuser Basel-Stadt"
           • sammlung is set to "Immobilienbewirtschaftung"
           • dienststelle is changed to "Immobilien Basel-Stadt"
      - If dienststelle equals "Erziehungsdepartement", it is cleared.
      - For the "Parlamentsdienst" scenario:
           • If dienststelle equals "Parlamentsdienst des Grossen Rates", it is changed to "Grosser Rat"
           • If sammlung equals "Parlamentsdienst des Grossen Rates", it is changed to "Parlamentsdienst"
           • If departement equals "Grosser Rat", it is changed to "Parlament"
      - If departement equals "Öffentlich-rechtliche Organisation", it is changed to 
        "Institutionen mit staatlichen Delegierten"
      - For municipal cases:
           • "Gemeinde Bettingen" becomes "Einwohnergemeinde Bettingen"
           • "Gemeinde Riehen" becomes "Einwohnergemeinde Riehen"
           • If dienststelle becomes "Einwohnergemeinde Riehen", then sammlung is set to "Gemeindeverwaltung"
           • If departement is one of {"Gemeinde Riehen", "Gemeinde Bettingen", "Bürgergemeinde der Stadt Basel"},
             it is changed to "Gemeindebehörden"
      - For cultural institutions:
           • If dienststelle is either "Museum der Kulturen Basel" or "Staatsarchiv Basel-Stadt",
             it is changed to "Abteilung Kultur"
           • If sammlung equals "Staatsarchiv Basel-Stadt", it is changed to "Staatsarchiv"
      - For OGD:
           • "Fachstelle für OGD Basel-Stadt" in dienststelle becomes "Statistisches Amt"
           • "Fachstelle für OGD Basel-Stadt" in sammlung becomes "Fachstelle OGD"
      - If dienststelle equals "Stadtreinigung", it is changed to "Tiefbauamt"
      - Standardizes official names by adding abbreviations:
           • "Amt für Beistandschaften und Erwachsenenschutz" becomes "Amt für Beistandschaften und Erwachsenenschutz ABES"
           • "Amt für Sozialbeiträge" becomes "Amt für Sozialbeiträge (ASB)"
           • "Amt für Umwelt und Energie" becomes "Amt für Umwelt und Energie (AUE)"
           • "Amt für Wirtschaft und Arbeit" becomes "Amt für Wirtschaft und Arbeit (AWA)"
           • "Basler Verkehrs-Betriebe" becomes "Basler Verkehrs-Betriebe (BVB)"
           • "Gebäude- und Wohnungsregister" becomes "Gebäude- und Wohnungsregister (GWR)"

    Args:
        departement (str): The originally extracted departement.
        dienststelle (str): The originally extracted dienststelle.

    Returns:
        tuple: A tuple containing (departement, dienststelle, sammlung, subsammlung).
    """
    sammlung = ""
    subsammlung = ""
    
    # Rule 1: If dienststelle is in this special set, set sammlung to its value.
    special_sammlung_values = {
        "Fachstelle für OGD Basel-Stadt",
        "Parlamentsdienst des Grossen Rates",
        "Stadtreinigung",
        "Museum der Kulturen Basel",
        "Staatsarchiv Basel-Stadt"
    }
    if dienststelle in special_sammlung_values:
        sammlung = dienststelle

    # Rule 2: "Immobilien Basel-Stadt" scenario
    if dienststelle == "Parkhäuser Basel-Stadt":
        subsammlung = "Parkhäuser Basel-Stadt"
        sammlung = "Immobilienbewirtschaftung"
        dienststelle = "Immobilien Basel-Stadt"

    # Rule 3: "Erziehungsdepartement" case
    if dienststelle == "Erziehungsdepartement":
        dienststelle = ""

    # Rule 4: "Parlamentsdienst" case
    if dienststelle == "Parlamentsdienst des Grossen Rates":
        dienststelle = "Grosser Rat"
    if sammlung == "Parlamentsdienst des Grossen Rates":
        sammlung = "Parlamentsdienst"
    if departement == "Grosser Rat":
        departement = "Parlament"

    # Rule 5: Transformation for öffentlich-rechtliche Organisation
    if departement == "Öffentlich-rechtliche Organisation":
        departement = "Institutionen mit staatlichen Delegierten"

    # Rule 6: Gemeindebehörden
    if dienststelle == "Gemeinde Bettingen":
        dienststelle = "Einwohnergemeinde Bettingen"
    if dienststelle == "Gemeinde Riehen":
        dienststelle = "Einwohnergemeinde Riehen"
    if dienststelle == "Einwohnergemeinde Riehen":
        sammlung = "Gemeindeverwaltung"
    if departement in {"Gemeinde Riehen", "Gemeinde Bettingen", "Bürgergemeinde der Stadt Basel"}:
        departement = "Gemeindebehörden"

    # Rule 7: Museum der Kulturen and Staatsarchiv
    if dienststelle in {"Museum der Kulturen Basel", "Staatsarchiv Basel-Stadt"}:
        dienststelle = "Abteilung Kultur"
    if sammlung == "Staatsarchiv Basel-Stadt":
        sammlung = "Staatsarchiv"

    # Rule 8: OGD
    if dienststelle == "Fachstelle für OGD Basel-Stadt":
        dienststelle = "Statistisches Amt"
    if sammlung == "Fachstelle für OGD Basel-Stadt":
        sammlung = "Fachstelle OGD"

    # Rule 9: "Stadtreinigung" case
    if dienststelle == "Stadtreinigung":
        dienststelle = "Tiefbauamt"
    
    # Rule 10: Non-Departement case
    if dienststelle in {
        "Bundesamt für Gesundheit BAG",
        "Bundesamt für Strassen ASTRA",
        "Bundesamt für Umwelt BAFU",
        "ETH Zurich, Department of Biosystems Science and Engineering",
        "MeteoSchweiz",
        "meteoblue AG",
        "OpenZH",
        "Nomenklaturkommission"
        }:
        departement = "Sonstige Organisationen und Firmen"
        
    # Rule 11: Standardize names with official abbreviations
    # Department name standardization
    # None yet.
    
    # Dienststelle name standardization
    if dienststelle == "Amt für Beistandschaften und Erwachsenenschutz":
        dienststelle = "Amt für Beistandschaften und Erwachsenenschutz ABES"
    if dienststelle == "Amt für Sozialbeiträge":
        dienststelle = "Amt für Sozialbeiträge (ASB)"
    if dienststelle == "Amt für Umwelt und Energie":
        dienststelle = "Amt für Umwelt und Energie (AUE)"
    if dienststelle == "Amt für Wirtschaft und Arbeit":
        dienststelle = "Amt für Wirtschaft und Arbeit (AWA)"
    if dienststelle == "Basler Verkehrs-Betriebe":
        dienststelle = "Basler Verkehrs-Betriebe (BVB)"
        
    # Sammlung name standardization
    if sammlung == "Gebäude- und Wohnungsregister":
        sammlung = "Gebäude- und Wohnungsregister (GWR)"

    return departement, dienststelle, sammlung, subsammlung

def create_path(json_data: Dict[str, Any]) -> str:
    """
    Constructs a hierarchical path for a dataset based on the provided metadata.
    
    The path is built in the following format:
        "Departement/Dienststelle/Sammlung/Subsammlung"
    For datasets from non-departement publishers, the Departement is overridden
    to "Sonstige Organisationen und Firmen".
    """
    # Extract Departement from the custom section
    departement_field = json_data['custom']['publizierende-organisation']
    departement = get_field_value(departement_field)
    
    # Extract the original Dienststelle from the default section
    dienststelle_field = json_data['default']['publisher']
    original_dienststelle = get_field_value(dienststelle_field)
    
    # Remove unwanted publisher grouping and apply standard special-case transformations.
    departement, dienststelle, sammlung, subsammlung = apply_special_cases(departement, original_dienststelle)
    
    path_fields = []
    
    if departement:
        path_fields.append(departement)
        
        if dienststelle:
            path_fields.append(dienststelle)
            
            if sammlung:
                path_fields.append(sammlung)
                
                if subsammlung:
                    path_fields.append(subsammlung)
    
    return "/".join(path_fields)

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
