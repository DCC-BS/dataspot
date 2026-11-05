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
    class BaseDataspotClient {
        -base_url: str
        -database_name: str
        -scheme_name: str
        -scheme_name_short: str
        -ods_imports_collection_name: str
        -ods_imports_collection_path: str
        +auth: DataspotAuth
        +get_all_assets_from_scheme()
        +_get_asset()
        +_create_asset()
        +_update_asset()
        +_delete_asset()
        +_mark_asset_for_deletion()
        +require_scheme_exists()
        +ensure_ods_imports_collection_exists()
        +bulk_create_or_update_assets()
        +get_org_units_by_staatskalender_ids()
    }

    %% DNK Client
    class DNKClient {
        -org_handler: OrgStructureHandler
        -dataset_handler: DatasetHandler
        +create_dataset()
        +update_dataset()
        +delete_dataset()
        +mark_dataset_for_deletion()
        +bulk_create_or_update_datasets()
        +sync_org_units()
        +sync_datasets()
    }

    %% ODS Client
    class ODSClient {
        -explore_api_version: str
        +get_dataset_columns()
        +get_organization_data()
        +get_all_organization_data()
    }

    %% Handler Classes
    class BaseDataspotHandler {
        -client: BaseDataspotClient
        -mapping: BaseDataspotMapping
        -database_name: str
        -scheme_name: str
        -scheme_name_short: str
        -asset_id_field: str
        -asset_type_filter: function
        +_download_and_update_mappings()
        +update_mappings_before_upload()
        +update_mappings_after_upload()
        +bulk_create_or_update_assets()
    }
    
    class DatasetHandler {
        -mapping: DatasetMapping
        -asset_id_field: str
        -asset_type_filter: function
        -default_dataset_path_full: str
        +sync_datasets()
        +update_mappings_after_upload()
        +bulk_create_or_update_datasets()
        +create_dataset()
        +update_dataset()
        +create_or_update_dataset()
        +delete_dataset()
    }
    
    class OrgUnitChange {
        +staatskalender_id: str
        +title: str
        +change_type: str
        +details: Dict
    }
    
    class OrgStructureHandler {
        -mapping: OrgStructureMapping
        -asset_id_field: str
        -asset_type_filter: function
        -updater: OrgStructureUpdater
        +sync_org_units()
        +_check_for_duplicate_ids_in_ods_staatskalender_data()
        +_check_for_duplicate_ids_in_dataspot()
        +build_organization_hierarchy_from_ods_bulk()
        +bulk_create_or_update_organizational_units()
        +update_mappings_after_upload()
        +update_mappings_before_upload()
        -_fetch_current_org_units()
    }

    %% Org Structure Helper Classes
    class OrgStructureTransformer {
        +transform_to_layered_structure()
        +build_organization_lookup()
        +find_root_nodes()
        +build_path_components()
    }

    class OrgStructureComparer {
        +compare_structures()
        +check_for_unit_changes()
        +generate_sync_summary()
        +generate_detailed_sync_report()
    }

    class OrgStructureUpdater {
        -client: BaseDataspotClient
        -database_name: str
        +apply_changes()
        -_process_deletions()
        -_process_updates()
        -_process_specific_changes()
        -_create_update_data()
        -_process_creations()
    }

    %% Mapping Classes    
    class BaseDataspotMapping {
        -database_name: str
        -csv_file_path: str
        -mapping: Dict
        -_id_field_name: str
        -_file_prefix: str
        -_scheme: str
        +id_field_name: property
        +csv_headers: property
        +save_to_csv()
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
        -_is_valid_uuid()
    }

    class DatasetMapping {
        -_id_field_name: str = "ods_id"
        -_file_prefix: str = "ods-dataspot"
        -_scheme: str
    }

    class OrgStructureMapping {
        -_id_field_name: str = "staatskalender_id"
        -_file_prefix: str = "staatskalender-dataspot"
        -_scheme: str
    }
    
    %% Dataset Classes
    class Dataset {
        <<abstract>>
        +name: str
        +_type: str
        +to_json()*
    }

    class BasicDataset {
        +kurzbeschreibung: str
        +beschreibung: str
        +schluesselwoerter: List[str]
        +synonyme: List[str]
        +aktualisierungszyklus: str
        +geographische_dimension: str
        +publikationsdatum: int
        +archivierung_details: str
        +archivierung_begruendung: str
        +nutzungseinschraenkung: str
        +art_der_historisierung: str
        +aufbewahrungsfrist_jahre: int
        +begruendung_aufbewahrungsfrist: str
        +to_json()
        +from_json()
    }

    class OGDDataset {
        +lizenz: List[str]
        +nutzungsrechte: str
        +datenportal_link: str
        +datenportal_identifikation: str
        +tags: List[str]
        +stereotype: str = "OGD"
    }

    %% Helper Functions
    class helpers {
        <<module>>
        +url_join()
        +get_uuid_from_response()
        +escape_special_chars()
        +unescape_path_components()
        +parse_iso_date_utc_midnight_ms()
    }

    %% HTTP Utilities
    class common {
        <<module>>
        +requests_get()
        +requests_post()
        +requests_put()
        +requests_patch()
        +requests_delete()
        -_print_potential_error_messages()
    }

    %% Retry Utilities
    class retry {
        <<function>>
        +retry(ExceptionToCheck, tries, delay, backoff, logger)
    }

    %% Transformer Function Module
    class dataset_transformer {
        <<module>>
        +transform_ods_to_dnk()
        +_get_field_value()
    }

    %% Relationships
    BaseDataspotClient o-- DataspotAuth : uses
    DNKClient --|> BaseDataspotClient : extends
    DNKClient o-- OrgStructureHandler : uses
    DNKClient o-- DatasetHandler : uses
    BaseDataspotClient ..> common : uses
    DataspotAuth ..> common : uses
    common ..> retry : uses
    
    %% Dataset relationships
    Dataset <|-- BasicDataset : extends
    BasicDataset <|-- OGDDataset : extends
    dataset_transformer ..> OGDDataset : creates
    ODSClient ..> dataset_transformer : uses
    dataset_transformer ..> helpers : uses
    DatasetHandler ..> Dataset : uses
    
    %% Handler relationships
    BaseDataspotHandler o-- BaseDataspotClient : uses
    BaseDataspotHandler o-- BaseDataspotMapping : uses
    DatasetHandler --|> BaseDataspotHandler : extends
    OrgStructureHandler --|> BaseDataspotHandler : extends
    OrgStructureHandler ..> OrgUnitChange : uses
    OrgStructureHandler o-- OrgStructureUpdater : contains
    OrgStructureHandler ..> OrgStructureTransformer : uses
    OrgStructureHandler ..> OrgStructureComparer : uses
    
    %% OrgStructure Helper relationships
    OrgStructureUpdater o-- BaseDataspotClient : uses
    OrgStructureUpdater ..> OrgUnitChange : uses
    OrgStructureComparer ..> OrgUnitChange : creates
    OrgStructureTransformer ..> helpers : uses
    OrgStructureUpdater ..> helpers : uses
    
    %% Mapping relationships
    BaseDataspotMapping <|-- DatasetMapping : extends
    BaseDataspotMapping <|-- OrgStructureMapping : extends
    DatasetHandler *-- DatasetMapping : contains
    OrgStructureHandler *-- OrgStructureMapping : contains
    
    %% Helper relationships
    BaseDataspotClient ..> helpers : uses
    ODSClient ..> helpers : uses
    OrgStructureHandler ..> helpers : uses
    DatasetHandler ..> helpers : uses
```

### Key Components:

1. **Authentication (DataspotAuth)**: Handles OAuth token management for Dataspot API access.

2. **Clients**:
   - **BaseDataspotClient**: Base class providing common functionality for Dataspot API interaction.
   - **DNKClient**: Extends `BaseDataspotClient` to specifically work with the Datennutzungskatalog (DNK). Delegates handling to specialized handlers.
   - **ODSClient**: Interfaces with the OpenDataSoft API to retrieve dataset and organization information.

3. **Handlers**:
   - **BaseDataspotHandler**: Base class for handlers that manage different types of assets in Dataspot.
   - **DatasetHandler**: Extends `BaseDataspotHandler` to handle dataset synchronization operations. Contains the `DatasetMapping` class for managing ODS ID to Dataspot UUID mappings.
   - **OrgStructureHandler**: Extends `BaseDataspotHandler` to handle organizational unit synchronization operations. Contains the `OrgStructureMapping` class for managing Staatskalender ID to Dataspot UUID mappings. Uses specialized helper classes for different aspects of organizational structure management.

4. **Org Structure Helpers**:
   - **OrgStructureTransformer**: Handles transformation of organizational structure data between different formats, particularly from ODS to Dataspot format.
   - **OrgStructureComparer**: Compares organizational structures and identifies changes needed, generating OrgUnitChange objects.
   - **OrgStructureUpdater**: Handles applying changes to organizational units in Dataspot, processing creations, updates, and deletions.
   - **OrgUnitChange**: Named tuple class used to track changes to organizational units during synchronization.

5. **Data Models**:
   - **Dataset**: Abstract base class for all dataset types.
   - **BasicDataset**: Extends Dataset with basic metadata fields.
   - **OGDDataset**: Extends BasicDataset with Open Government Data specific fields.

6. **Mapping**:
   - **BaseDataspotMapping**: Base class providing mapping functionality for external IDs to Dataspot UUIDs.
   - **DatasetMapping**: Extends BaseDataspotMapping to specifically map ODS dataset IDs to Dataspot UUIDs.
   - **OrgStructureMapping**: Extends BaseDataspotMapping to map Staatskalender organization IDs to Dataspot UUIDs.

7. **HTTP Utilities**:
   - **common**: Module providing standardized HTTP request functions with consistent error handling.
   - **retry**: Decorator function implementing retry logic for HTTP requests that may experience transient failures.

8. **Utility Modules**:
   - **helpers**: Module containing utility functions for URL manipulation, response parsing, and special character handling.
   - **dataset_transformer**: Module containing functions to convert ODS metadata format to Dataspot DNK format.

### Data Flow:

1. **Dataset Synchronization**:
   - The process begins with fetching dataset metadata from OpenDataSoft using ODSClient.
   - This metadata is transformed into Dataspot's format using functions in the dataset_transformer module.
   - The DNKClient delegates operations to DatasetHandler, which uses its internal DatasetMapping to track relationships between systems.
   - DatasetHandler creates, updates, or deletes datasets in Dataspot's DNK, with each operation requiring authentication via DataspotAuth.
   - For bulk operations, multiple datasets can be processed in a single API call.

2. **Organizational Unit Synchronization**:
   - Organization data is retrieved from OpenDataSoft via ODSClient.
   - DNKClient delegates operations to OrgStructureHandler, which orchestrates the synchronization process.
   - OrgStructureTransformer converts flat organization data into a hierarchical structure.
   - OrgStructureComparer identifies differences between source and current organizational structures.
   - OrgStructureUpdater applies identified changes to Dataspot.
   - OrgStructureHandler uses its internal OrgStructureMapping to maintain mappings between Staatskalender IDs and Dataspot UUIDs.
   - OrgUnitChange instances track creations, updates, and deletions of organizational units during synchronization.

This architecture enables synchronization of both datasets and organizational units between OpenDataSoft and Dataspot while maintaining mappings between the systems.

### Directory Structure

The source code follows this organization:
- `src/`
  - `clients/`: Contains client implementations for different services
  - `common/`: Contains shared utility functions
  - `mapping_handlers/`: Contains handlers for various types of data mapping
    - `org_structure_helpers/`: Contains specialized components for organizational structure handling
  - Core modules like dataset models and transformers

### Note on Debugging Files

The repository contains debugging and temporary files such as `renato_debugging.py` and various `todos_*.txt` files which are not part of the core system architecture. These files contain temporary debugging code and should not be considered part of the production system.
