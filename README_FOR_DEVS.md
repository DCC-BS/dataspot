## System Architecture Overview

This project helps keep data in sync between OpenDataSoft (ODS) and Dataspot's Datennutzungskatalog (DNK). The diagram below shows the main parts of the system and how they connect.

```mermaid
classDiagram
    %% Core Authentication
    class DataspotAuth {
        -token_url: str
        -client_id: str
        -username: str
        -password: str
        -token: str
        -token_expires_at: datetime
        +get_bearer_token()
        +get_headers()
        -_is_token_valid()
        -_request_new_bearer_token()
    }

    %% Base Client
    class DataspotClientInterface {
        <<interface>>
        +require_scheme_exists()*
        +create_resource()*
        +bulk_create_or_update_resources()*
        +update_resource()*
        +delete_resource()*
        +get_resource_if_exists()*
    }

    class BaseDataspotClient {
        <<abstract>>
        -auth: DataspotAuth
        -base_url: str
        -database_name: str
        -ods_imports_collection_name: str
        +require_scheme_exists()* 
        +create_resource()
        +bulk_create_or_update_resources()
        +update_resource()
        +delete_resource()
        +get_resource_if_exists()
    }

    %% DNK Client
    class DNKClient {
        -scheme_name: str
        -mapping: ODSDataspotMapping
        -org_mapping: StaatskalenderDataspotMapping
        +require_scheme_exists()
        +ensure_ods_imports_collection_exists()
        +create_dataset()
        +update_dataset()
        +create_or_update_dataset()
        +bulk_create_or_update_datasets()
        +delete_dataset()
        +update_mappings_from_upload()
        -_download_and_update_mappings()
        %% Organizational unit methods
        +transform_organization_for_bulk_upload()
        +bulk_create_or_update_organizational_units()
        +build_organization_hierarchy_from_ods_bulk()
        +get_validated_staatskalender_url()
        +update_staatskalender_mappings_from_upload()
        -_download_and_update_staatskalender_mappings()
    }

    %% ODS Client
    class ODSClient {
        -explore_api_version: str
        +get_dataset_columns()
        +get_organization_data()
    }

    %% Mapping Classes
    class DataspotMappingInterface {
        <<interface>>
        +id_field_name: str
        +csv_headers: List[str]
        +get_entry()*
        +add_entry()*
        +remove_entry()*
        +get_type()*
        +get_uuid()*
        +get_inCollection()*
        +get_all_entries()*
        +get_all_ids()*
    }
    
    class BaseDataspotMapping {
        -database_name: str
        -csv_file_path: str
        -mapping: Dict
        -_id_field_name: str
        -_file_prefix: str
        +get_entry()
        +add_entry()
        +remove_entry()
        +get_type()
        +get_uuid()
        +get_inCollection()
        +get_all_entries()
        +get_all_ids()
        -_get_mapping_file_path()
        -_load_mapping()
        -_save_mapping()
        -_is_valid_uuid()
    }

    class ODSDataspotMapping {
        +get_all_ods_ids()
    }

    class StaatskalenderDataspotMapping {
        +get_all_staatskalender_ids()
    }
    
    %% Dataset Classes
    class Dataset {
        <<abstract>>
        +name: str
        +_type: str
        +to_json()*
    }

    class BasicDataset {
        +beschreibung: str
        +schluesselwoerter: List[str]
        +aktualisierungszyklus: str
        +geographische_dimension: str
        +publikationsdatum: int
        +to_json()
        +from_json()
    }

    class OGDDataset {
        +lizenz: List[str]
        +nutzungsrechte: str
        +datenportal_identifikation: str
        +tags: List[str]
        +stereotype: str
    }

    %% HTTP Request Wrappers
    class CommonRequestWrappers {
        <<static>>
        +requests_get()
        +requests_post()
        +requests_put()
        +requests_patch()
        +requests_delete()
    }

    %% Retry Utilities
    class RetryDecorator {
        <<static>>
        +retry()
    }

    %% Helper Functions
    class Helpers {
        <<static>>
        +url_join()
        +get_uuid_from_response()
        +escape_special_chars()
    }

    %% Transformer Functions
    class DatasetTransformer {
        <<static>>
        +transform_ods_to_dnk()
        +_iso_8601_to_unix_timestamp()
        +_get_field_value()
    }

    %% Relationships
    BaseDataspotClient o-- DataspotAuth : uses
    DNKClient --|> BaseDataspotClient : extends
    DNKClient o-- ODSDataspotMapping : uses
    DNKClient o-- StaatskalenderDataspotMapping : uses
    DNKClient ..> Helpers : uses
    DNKClient ..> Dataset : creates/updates
    BaseDataspotClient ..> CommonRequestWrappers : uses
    DataspotAuth ..> CommonRequestWrappers : uses
    CommonRequestWrappers ..> RetryDecorator : uses
    Dataset <|-- BasicDataset : extends
    BasicDataset <|-- OGDDataset : extends
    DatasetTransformer ..> OGDDataset : creates
    ODSClient ..> DatasetTransformer : data feeds into
    
    %% New mapping relationships
    DataspotMappingInterface <|.. BaseDataspotMapping : implements
    BaseDataspotMapping <|-- ODSDataspotMapping : extends
    BaseDataspotMapping <|-- StaatskalenderDataspotMapping : extends

    %% New client relationships
    DataspotClientInterface <|.. BaseDataspotClient : implements
```

### Key Components:

1. **Authentication (DataspotAuth)**: Handles OAuth token management for Dataspot API access.

2. **Clients**:
   - **DataspotClientInterface**: Interface defining the core operations for Dataspot API clients.
   - **BaseDataspotClient**: Abstract base class implementing the `DataspotClientInterface` and providing common functionality for Dataspot API interaction.
   - **DNKClient**: Extends `BaseDataspotClient` to specifically work with the Datennutzungskatalog (DNK). Handles both dataset and organizational unit operations.
   - **ODSClient**: Interfaces with the OpenDataSoft API to retrieve dataset information.

3. **Data Models**:
   - **Dataset**: Abstract base class for all dataset types.
   - **BasicDataset**: Extends Dataset with basic metadata fields.
   - **OGDDataset**: Extends BasicDataset with Open Government Data specific fields.

4. **Mapping**:
   - **DataspotMappingInterface**: Abstract interface defining the contract for all mapping classes.
   - **BaseDataspotMapping**: Concrete implementation of the interface providing common functionality for all mapping types.
   - **ODSDataspotMapping**: Extends BaseDataspotMapping to specifically map ODS dataset IDs to Dataspot UUIDs.
   - **StaatskalenderDataspotMapping**: Extends BaseDataspotMapping to map Staatskalender organization IDs to Dataspot UUIDs.

5. **HTTP Utilities**:
   - **CommonRequestWrappers**: Provides standardized HTTP request functions with consistent error handling.
   - **RetryDecorator**: Implements retry logic for HTTP requests that may experience transient failures.

6. **Transformation (DatasetTransformer)**: Contains functions to convert ODS metadata format to Dataspot DNK format.

7. **Helpers**: Utility functions for URL manipulation, response parsing, and special character handling.

### Data Flow:

1. **Dataset Synchronization**:
   - The process begins with fetching dataset metadata from OpenDataSoft using ODSClient.
   - This metadata is transformed into Dataspot's format using functions in DatasetTransformer.
   - The DNKClient uses ODSDataspotMapping to track relationships between systems.
   - DNKClient creates, updates, or deletes datasets in Dataspot's DNK, with each operation requiring authentication via DataspotAuth.
   - For bulk operations, multiple datasets can be processed in a single API call.

2. **Organizational Unit Synchronization**:
   - Organization data is retrieved from OpenDataSoft.
   - DNKClient transforms the flat organization data into a hierarchical structure.
   - The hierarchical data is uploaded to Dataspot level by level to preserve parent-child relationships.
   - StaatskalenderDataspotMapping is used to track the mapping between Staatskalender IDs and Dataspot UUIDs.

This architecture enables synchronization of both datasets and organizational units between OpenDataSoft and Dataspot while maintaining mappings between the systems.

### Note on Debugging Code

The repository contains a `renato_debugging.py` file which is not part of the core system architecture. This file contains temporary debugging code and should not be considered part of the production system.
