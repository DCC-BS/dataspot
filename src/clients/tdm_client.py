from typing import List, Dict, Any
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

    def get_compositions_with_cache(self) -> List[Dict[str, Any]]:
        """
        Get Composition objects (UmlClass with stereotype='ogd_dataset') with caching support.
        
        Uses SQL Query API to fetch only the required assets with all filtering done in the query.
        No in-memory filtering is needed.
            
        Returns:
            List[Dict[str, Any]]: List of Composition assets
        """
        # Check if cache is populated
        if self._compositions_cache is not None:
            logging.info(f"Using cached Compositions from {self.scheme_name_short} scheme ({len(self._compositions_cache)} assets)")
            return list(self._compositions_cache)
        
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
        
        # Convert SQL results to match Download API format (snake_case to camelCase)
        self._compositions_cache = []
        for row in results:
            asset = {
                'id': row.get('id'),
                '_type': row.get('_type'),
                'inCollection': row.get('in_collection'),
                'label': row.get('label'),
                'stereotype': row.get('stereotype'),
                'status': row.get('status'),
                'odsDataportalId': strip_quotes(row.get('ods_dataportal_id'))
            }
            self._compositions_cache.append(asset)
        
        logging.info(f"Cached {len(self._compositions_cache)} Compositions from {self.scheme_name_short} scheme")
        
        return list(self._compositions_cache)

    def clear_compositions_cache(self) -> None:
        """
        Clear the Compositions cache, forcing a fresh download on the next request.
        
        Call this method if you know the Composition data has changed externally
        or after making changes to Composition objects.
        """
        self._compositions_cache = None
        logging.info(f"Cleared Compositions cache for {self.scheme_name_short} scheme")
