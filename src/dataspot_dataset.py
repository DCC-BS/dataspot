from dataclasses import dataclass, field, fields
from typing import List, Optional, Dict, Any
from abc import ABC, abstractmethod

# TODO: Denote somewhere in dataspot that we have timestamps in UTC
# Field types guide:
# 1. Final (immutable) fields:
#    _type: str = field(default="Dataset", init=False)
#
# 2. Mandatory fields:
#    name: str = field(metadata={"json_key": "label"})
#
# 3. Optional fields:
#    kurzbeschreibung: Optional[str] = field(default=None, metadata={"json_key": "title"})
#
# 4. Optional fields that are Custom Properties:
#    tags: Optional[List[str]] = field(default=None, metadata={'json_key': 'TAG', 'custom_property': True})

@dataclass
class Dataset(ABC):
    """Base class for all dataset types, serving as a common type annotation for BasicDataset, OGDDataset, and other dataset variants. This class CANNOT be instantiated directly."""
    name: str = field(metadata={"json_key": "label"})
    _PATH: str = field()

    _type: str = field(default="Dataset", init=False)

    @abstractmethod
    def to_json(self) -> Dict[str, Any]:
        """
        Must be implemented by subclasses.
        """
        raise NotImplementedError(f"The method to_json cannot be called from the abstract Dataset class directly!")

    def get_departement_dienststelle_sammlung_subsammlung(self) -> (str, Optional[str], Optional[str], Optional[str]):
        """
        Extracts the departement, dienststelle, sammlung, and subsammlung from the _PATH field.
        
        Parts are processed sequentially - each part is only considered if all previous parts exist.
        
        Returns:
            tuple: (departement, dienststelle, sammlung, subsammlung) where all except departement may be None.
            
        Raises:
            ValueError: If the department (first part) is empty.
        """
        parts = self._PATH.strip('/').split('/')
        
        # Department is always the first part and must not be empty
        if not parts[0]:
            raise ValueError("Dienststelle cannot be empty!")
            
        departement = parts[0]
        dienststelle = None
        sammlung = None
        subsammlung = None
        
        # Check for dienststelle
        if len(parts) >= 2:
            dienststelle = parts[1]
            
            # Check for sammlung (only if we have dienststelle)
            if len(parts) >= 3:
                sammlung = parts[2]
                
                # Check for subsammlung (only if we have sammlung)
                if len(parts) >= 4:
                    subsammlung = parts[3]
        
        return departement, dienststelle, sammlung, subsammlung

    # TODO (large language model): add unit tests for this.
    def to_dict(self):
        exit("TODO: Please implement me.")
        pass


@dataclass
class BasicDataset(Dataset):
    kurzbeschreibung: Optional[str] = field(default=None, metadata={'json_key': 'title'})
    beschreibung: Optional[str] = field(default=None, metadata={'json_key': 'description'})
    schluesselwoerter: Optional[List[str]] = field(default=None, metadata={'json_key': 'tags'})
    synonyme: Optional[List[str]] = field(default=None, metadata={'json_key': 'synonyms'})
    aktualisierungszyklus: Optional[str] = field(default=None, metadata={'json_key': 'accrualPeriodicity'})
    identifikation: Optional[str] = field(default=None, metadata={'json_key': 'identifier'})
    # TODO: zeitliche_dimension_beginn
    # TODO: zeitliche_dimension_ende
    geographische_dimension: Optional[str] = field(default=None, metadata={'json_key': 'spatial'}) # TODO: Check
    # TODO: vertraulichkeit -> Immer öffentlich bei OGD
    #schutzbedarfsstufen -> ???
    #letzte_aktualisierung -> ???
    publikationsdatum: Optional[int] = field(default=None, metadata={'json_key': 'PD', 'custom_property': True})
    #archivierung -> ???
    archivierung_details: Optional[str] = field(default=None, metadata={'json_key': 'ARCHDET'})
    archivierung_begruendung: Optional[str] = field(default=None, metadata={'json_key': 'ARCHBEGR'})
    nutzungseinschraenkung: Optional[str] = field(default=None, metadata={'json_key': 'NE'})
    #historisierung: bool = field(default=False, metadata={'json_key': 'HIST'})
    #historisierung_seit_wann
    art_der_historisierung: Optional[str] = field(default=None, metadata={'json_key': 'HISTART'})
    aufbewahrungsfrist_jahre: Optional[int] = field(default=None, metadata={'json_key': 'ABF'})
    begruendung_aufbewahrungsfrist: Optional[str] = field(default=None, metadata={'json_key': 'BEGRABF'})

    def to_json(self) -> Dict[str, Any]:
        """
        Serializes the instance to a JSON-compatible dictionary with correct JSON keys.
        """
        json_dict = {}
        custom_properties = {}
        
        for f in fields(self):
            value = getattr(self, f.name)
            if value is not None:  # Only include non-None values
                json_key = f.metadata.get("json_key", f.name)
                is_custom_property = f.metadata.get("custom_property", False)

                # Do not include "_PATH" in the dict
                if json_key == "_PATH":
                    continue
                
                if is_custom_property:
                    custom_properties[json_key] = value
                else:
                    json_dict[json_key] = value
        
        # Add custom properties if any exist
        if custom_properties:
            json_dict["customProperties"] = custom_properties
            
        return json_dict

    @classmethod
    def from_json(cls, json_data: Dict[str, Any], _PATH: str) -> 'BasicDataset':
        """
        Deserializes JSON data into a dataset instance.
        """
        # TODO: Handle the historisierung case, since in dataspot this is lowercase.
        init_data = {'_PATH': _PATH}
        
        # Map JSON keys to attribute names
        json_key_to_attr = {}
        custom_property_keys = {}
        
        for f in fields(cls):
            json_key = f.metadata.get("json_key", f.name)
            is_custom_property = f.metadata.get("custom_property", False)
            
            if is_custom_property:
                custom_property_keys[json_key] = f.name
            else:
                json_key_to_attr[json_key] = f.name
        
        # Process regular properties
        for key, value in json_data.items():
            if key == "customProperties":
                continue  # Handle custom properties separately
            attr = json_key_to_attr.get(key, key)
            init_data[attr] = value
        
        # Process custom properties if they exist
        if "customProperties" in json_data and isinstance(json_data["customProperties"], dict):
            custom_props = json_data["customProperties"]
            for key, value in custom_props.items():
                attr = custom_property_keys.get(key)
                if attr:
                    init_data[attr] = value
        
        return cls(**init_data)

@dataclass
class OGDDataset(BasicDataset):
    #anhaefungsperiodizität__TMP - GELÖSCHT
    # TODO: lizenz
    # TODO: rechte
    # TODO: themen
    #herausgeber - GELÖSCHT
    # TODO: referenz
    # TODO: zuschreibungen
    #publizierende_organisation - GELÖSCHT
    # TODO: datenportal_identifikation
    tags: Optional[List[str]] = field(default=None, metadata={'json_key': 'TAG', 'custom_property': True})

    # Immutable fields
    stereotype: str = field(default="OGD", init=False)

# TODO: Similarly for GeoDataset and OGDGeoDataset
