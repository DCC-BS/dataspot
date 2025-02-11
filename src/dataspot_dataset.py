from dataclasses import dataclass, field, fields
from typing import List, Optional, Dict, Any
from abc import ABC, abstractmethod

@dataclass
class Dataset(ABC):
    """Base class for all dataset types, serving as a common type annotation for BasicDataset, OGDDataset, and other dataset variants. This class CANNOT be instantiated directly."""
    name: str = field(metadata={"json_key": "label"})

    @abstractmethod
    def to_json(self) -> Dict[str, Any]:
        """
        Must be implemented by subclasses.
        """
        pass

@dataclass
class BasicDataset(Dataset):
    kurzbeschreibung: Optional[str] = field(default=None, metadata={"json_key": "title"})
    # TODO: Add all other fields

    # Immutable fields
    _type: str = field(default="Dataset", init=False)

    def to_json(self) -> Dict[str, Any]:
        """
        Serializes the instance to a JSON-compatible dictionary with correct JSON keys.
        """
        json_dict = {}
        for f in fields(self):
            value = getattr(self, f.name)
            if value is not None:  # Only include non-None values
                json_key = f.metadata.get("json_key", f.name)
                json_dict[json_key] = value
        return json_dict

    @classmethod
    def from_json(cls, json_data: Dict[str, Any]) -> 'BasicDataset':
        """
        Deserializes JSON data into a dataset instance.
        """
        init_data = {}
        json_key_to_attr = {f.metadata.get("json_key", f.name): f.name for f in fields(cls)}
        for key, value in json_data.items():
            attr = json_key_to_attr.get(key, key)
            init_data[attr] = value
        return cls(**init_data)

@dataclass
class OGDDataset(BasicDataset):
    ogd_specific_field: Optional[str] = None
    # TODO: Add all other fields

    # Immutable fields
    stereotype: str = field(default="OGD", init=False)


# TODO: Similarly for GeoDataset and OGDGeoDataset
