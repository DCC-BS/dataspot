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
tdm_scheme_name = 'Datenobjekte'
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
