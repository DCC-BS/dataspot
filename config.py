# Dataspot configuration variables

# Database name
database_name = 'prod'

# Base URL
base_url = 'https://datenkatalog.bs.ch'
database_name_prod = 'prod'

# Scheme names
dnk_scheme_name = 'Datenprodukte'
dnk_scheme_name_short = 'DNK'
fdm_scheme_name = 'Fachdaten'
fdm_scheme_name_short = 'FDM'
rdm_scheme_name = 'Referenzdaten'
rdm_scheme_name_short = 'RDM'
kv_scheme_name = 'Kennzahlen'
kv_scheme_name_short = 'KV'
datatype_scheme_name = 'Datentypen (technisch)'
datatype_scheme_name_short = 'DTM'
tdm_scheme_name = 'Datenbankobjekte'
tdm_scheme_name_short = 'TDM'
sk_scheme_name = 'Systeme'
sk_scheme_name_short = 'SK'

# Client-specific ODS Imports collection configurations, where needed. Empty list for the path means directly under scheme root
# DNK client configuration
dnk_ods_imports_collection_name = 'OGD-Datensätze aus ODS'
dnk_ods_imports_collection_path = ['Regierung und Verwaltung', 'Präsidialdepartement', 'Statistisches Amt', 'DCC Data Competence Center']

# TDM client configuration
tdm_ods_imports_collection_name = 'OGD-Datensätze aus ODS'
tdm_ods_imports_collection_path = []

# Validate that critical configuration values are present
assert base_url, "base_url must be set in config.py"
assert database_name, "database_name must be set in config.py"
assert database_name_prod, "database_name_prod must be set in config.py"
assert dnk_scheme_name, "dnk_scheme_name must be set in config.py"
assert dnk_scheme_name_short, "dnk_scheme_name_short must be set in config.py"
assert fdm_scheme_name, "fdm_scheme_name must be set in config.py"
assert fdm_scheme_name_short, "fdm_scheme_name_short must be set in config.py"
assert rdm_scheme_name, "rdm_scheme_name must be set in config.py"
assert rdm_scheme_name_short, "rdm_scheme_name_short must be set in config.py"
assert kv_scheme_name, "kv_scheme_name must be set in config.py"
assert kv_scheme_name_short, "kv_scheme_name_short must be set in config.py"
assert datatype_scheme_name, "datatype_scheme_name must be set in config.py"
assert datatype_scheme_name_short, "datatype_scheme_name_short must be set in config.py"
assert tdm_scheme_name, "tdm_scheme_name must be set in config.py"
assert tdm_scheme_name_short, "tdm_scheme_name_short must be set in config.py"
assert sk_scheme_name, "sk_scheme_name must be set in config.py"
assert sk_scheme_name_short, "sk_scheme_name_short must be set in config.py"
assert dnk_ods_imports_collection_name, "dnk_ods_imports_collection_name must be set in config.py"
assert type(dnk_ods_imports_collection_path) == type([]), "dnk_ods_imports_collection_path must be set in config.py"
assert tdm_ods_imports_collection_name, "tdm_ods_imports_collection_name must be set in config.py"
assert type(tdm_ods_imports_collection_path) == type([]), "tdm_ods_imports_collection_path must be set in config.py"
