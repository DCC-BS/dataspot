# Dataspot configuration variables

# Database name
database_name = 'prod'

# Base URL
base_url = 'https://bs.dataspot.io'
database_name_prod = 'prod'

# Scheme names
dnk_scheme_name = 'Datennutzungskatalog'
dnk_scheme_name_short = 'DNK'
fdm_scheme_name = 'Fachdatenmodell'
fdm_scheme_name_short = 'FDM'
rdm_scheme_name = 'Referenzdatenmodell'
rdm_scheme_name_short = 'RDM'
kv_scheme_name = 'Kennzahlenverzeichnis'
kv_scheme_name_short = 'KV'
datatype_scheme_name = 'Datentypmodell (technisch)'
datatype_scheme_name_short = 'DTM'
tdm_scheme_name = 'Technische Datenmodelle'
tdm_scheme_name_short = 'TDM'

# Default ODS Imports collection name
ods_imports_collection_name = 'OGD-Datensätze aus ODS'

# Default path for the ODS Imports collection within the DNK scheme.
# An empty list [] means it's directly under the scheme root.
# Example: ["Parent Folder", "Sub Folder"] places it under Parent Folder/Sub Folder.
ods_imports_collection_path = ['Regierung und Verwaltung', 'Präsidialdepartement', 'Statistisches Amt', 'DCC Data Competence Center']

# Validate that critical configuration values are present
assert base_url, "base_url must be set in config.py"
assert database_name_prod, "database_name_prod must be set in config.py"
assert database_name, "database_name must be set in config.py"
assert dnk_scheme_name, "dnk_scheme_name must be set in config.py"
assert rdm_scheme_name, "rdm_scheme_name must be set in config.py"
assert datatype_scheme_name, "datatype_scheme_name must be set in config.py"
assert tdm_scheme_name, "tdm_scheme_name must be set in config.py"
assert ods_imports_collection_name, "ods_imports_collection_name must be set in config.py"
