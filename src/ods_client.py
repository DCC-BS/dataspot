import ods_utils_py as ods_utils

class ODSClient:
    """Client for interacting with the ODS API."""

    def __init__(self):
        """
        Initialize the ODS client.

        """
        self.explore_api_version = 'v2.1'

    def get_dataset_columns(self, dataset_id: str) -> list:
        """
        Get the list of columns/fields for a dataset.
        
        Args:
            dataset_id (str, optional): The dataset ID. Defaults to None.
            
        Returns:
            list: List of column configurations for the dataset
            
        Raises:
            SystemExit: If neither or both dataset_id and dataset_uid are provided
            HTTPError: If the API request fails
        """
        if not dataset_id:
            exit("Error: dataset_id has to be specified.")
        
        # Use the Explore API v2.1 to get dataset information
        r = ods_utils.requests_get(url=f"https://data.bs.ch/api/explore/{self.explore_api_version}/catalog/datasets/{dataset_id}")
        r.raise_for_status()

        # Extract the field configurations from the response
        dataset_info = r.json()
        fields = dataset_info.get('fields', [])

        column_info = []
        for field in fields:
            column_data = {
                'label': field.get('label'),
                'name': field.get('name'),
                'type': field.get('type')
            }
            if field.get('type') == 'text' and 'semantic_type' in field and field.get('semantic_type') == 'identifier':
                column_data['type'] = 'identifier'
            column_info.append(column_data)



        return column_info