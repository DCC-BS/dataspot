from typing import Dict, Any, Optional, overload
import logging

import config
from src.clients.base_client import BaseDataspotClient
from src.clients.helpers import strip_quotes
from src.mapping_handlers.org_structure_handler import OrgStructureHandler
from src.mapping_handlers.dataset_composition_handler import DatasetCompositionHandler

class TDMClient(BaseDataspotClient):
    """Client for interacting with the TDM (Technisches Datenmodell) scheme."""
    
    def __init__(self):
        """
        Initialize the TDM client.
        """
        super().__init__(scheme_name=config.tdm_scheme_name,
                         scheme_name_short=config.tdm_scheme_name_short,
                         ods_imports_collection_name=config.tdm_ods_imports_collection_name,
                         ods_imports_collection_path=config.tdm_ods_imports_collection_path)
        
        # Initialize cache for Composition objects (UmlClass with stereotype='ogd_dataset')
        self._compositions_cache = None
        
        # Initialize the handlers
        self.org_handler = OrgStructureHandler(self)
        self.composition_handler = DatasetCompositionHandler(self)

    @overload
    def get_compositions_with_cache(self) -> Dict[str, Dict[str, Any]]: ...
    
    @overload
    def get_compositions_with_cache(self, odsDataportalId: str) -> Optional[Dict[str, Any]]: ...
    
    def get_compositions_with_cache(self, odsDataportalId: Optional[str] = None) -> Dict[str, Dict[str, Any]] | Optional[Dict[str, Any]]:
        """
        Get Composition objects (UmlClass with stereotype='ogd_dataset') with caching support.
        
        Uses SQL Query API to fetch only the required assets with all filtering done in the query.
        No in-memory filtering is needed.
        
        Args:
            odsDataportalId: Optional ODS ID to filter by. If provided, returns a single
                composition dict or None if not found. If not provided, returns all compositions.
            
        Returns:
            If odsDataportalId is None: Dict[str, Dict[str, Any]] - Dictionary of all compositions keyed by odsDataportalId
            If odsDataportalId is provided: Optional[Dict[str, Any]] - Single composition dict or None if not found
            
        Raises:
            ValueError: If duplicate odsDataportalId values are found in the data
        """
        # Check if cache is populated
        if self._compositions_cache is not None:
            logging.info(f"Using cached Compositions from {self.scheme_name_short} scheme ({len(self._compositions_cache)} assets)")
            if odsDataportalId is not None:
                return self._compositions_cache.get(odsDataportalId)
            return dict(self._compositions_cache)
        
        # Cache is empty, fetch using SQL Query API
        logging.info(f"Fetching Composition assets from {self.scheme_name_short} scheme using SQL Query API")
        
        query = """
            SELECT 
                cl.id,
                cl._type,
                cl.in_collection,
                cl.label,
                cl.stereotype,
                cl.status,
                cp.value AS ods_dataportal_id
            FROM 
                classifier_view cl
            JOIN
                customproperties_view cp ON cl.id = cp.resource_id AND cp.name = 'odsDataportalId'
            WHERE 
                cl._type = 'UmlClass'
                AND cl.stereotype = 'ogd_dataset'
                AND cp.value IS NOT NULL
                AND cl.status NOT IN ('INTERMINATION2', 'ARCHIVEMETA')
            ORDER BY
                cl.label
        """
        
        results = self.execute_query_api(sql_query=query)
        
        # Convert SQL results to dict keyed by odsDataportalId
        self._compositions_cache = {}
        for row in results:
            ods_id = strip_quotes(row.get('ods_dataportal_id'))
            
            # Check for duplicates
            if ods_id in self._compositions_cache:
                existing = self._compositions_cache[ods_id]
                raise ValueError(
                    f"Duplicate odsDataportalId '{ods_id}' found: "
                    f"existing composition '{existing.get('label')}' (id: {existing.get('id')}), "
                    f"new composition '{row.get('label')}' (id: {row.get('id')})"
                )
            
            asset = {
                'id': row.get('id'),
                '_type': row.get('_type'),
                'inCollection': row.get('in_collection'),
                'label': row.get('label'),
                'stereotype': row.get('stereotype'),
                'status': row.get('status'),
                'odsDataportalId': ods_id
            }
            self._compositions_cache[ods_id] = asset
        
        logging.info(f"Cached {len(self._compositions_cache)} Compositions from {self.scheme_name_short} scheme")
        
        if odsDataportalId is not None:
            return self._compositions_cache.get(odsDataportalId)
        return dict(self._compositions_cache)

    def clear_compositions_cache(self) -> None:
        """
        Clear the Compositions cache, forcing a fresh download on the next request.
        
        Call this method if you know the Composition data has changed externally
        or after making changes to Composition objects.
        """
        self._compositions_cache = None
        logging.info(f"Cleared Compositions cache for {self.scheme_name_short} scheme")
