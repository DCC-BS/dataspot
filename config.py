# Dataspot configuration variables

# Database name
database_name = 'prod'
test_database_name = 'int-gesetzessammlungen'

# Logging config
logging_for_prod = True
MAX_RETRIES_FOR_PROD = True

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
law_scheme_name = 'Gesetzessammlungen'
law_scheme_name_short = 'GS'

law_bs_collection_label = 'Systematische Gesetzessammlung Basel-Stadt'
law_ch_collection_label = 'Systematische Rechtssammlung Schweiz'

# Special names
tenant_name = "Mandant"
organizations_name = "Data%20Governance"

# System constants
huwise_system_uuid = 'e6dca403-8d39-4597-96ae-601a81d30e85'
law_bs_system_label = 'Systematische Gesetzessammlung Basel-Stadt'
law_ch_system_label = 'Systematische Rechtssammlung Schweiz'

# Client-specific ODS Imports collection configurations, where needed. Empty list for the path means directly under scheme root
# DNK client configuration
dnk_ods_imports_collection_name = 'OGD-Datensätze aus Huwise'
dnk_ods_imports_collection_path = ['Regierung und Verwaltung', 'Präsidialdepartement', 'Statistisches Amt', 'DCC Data Competence Center']

# TDM client configuration
tdm_ods_imports_collection_name = 'OGD-Datensätze in Huwise'
tdm_ods_imports_collection_path = ['Regierung und Verwaltung', 'Präsidialdepartement', 'Statistisches Amt', 'DCC Data Competence Center']
