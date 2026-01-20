{{
    config(
        description="Raw layer (SUS emergency care attendances and activity). 1:1 passthrough with cleaned column names. \nSource: DATA_LAKE.SUS_UNIFIED_ECDS.clinical.treatments.snomed \ndbt: source(''sus_ae'', ''clinical.treatments.snomed'') \nColumns:\n  ROWNUMBER_ID -> rownumber_id\n  PRIMARYKEY_ID -> primarykey_id\n  equivalent_ae_code -> equivalent_ae_code\n  time -> time\n  dmicImportLogId -> dmic_import_log_id\n  is_code_approved -> is_code_approved\n  SNOMED_ID -> snomed_id\n  date -> date\n  timestamp -> timestamp\n  code -> code"
    )
}}
select
    "ROWNUMBER_ID" as rownumber_id,
    "PRIMARYKEY_ID" as primarykey_id,
    "equivalent_ae_code" as equivalent_ae_code,
    "time" as time,
    "dmicImportLogId" as dmic_import_log_id,
    "is_code_approved" as is_code_approved,
    "SNOMED_ID" as snomed_id,
    "date" as date,
    "timestamp" as timestamp,
    "code" as code
from {{ source('sus_ae', 'clinical.treatments.snomed') }}
