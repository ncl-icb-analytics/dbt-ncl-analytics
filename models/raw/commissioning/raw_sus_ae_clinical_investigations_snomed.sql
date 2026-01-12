{{
    config(
        description="Raw layer (SUS emergency care attendances and activity). 1:1 passthrough with cleaned column names. \nSource: DATA_LAKE.SUS_UNIFIED_ECDS.clinical.investigations.snomed \ndbt: source(''sus_ae'', ''clinical.investigations.snomed'') \nColumns:\n  is_code_approved -> is_code_approved\n  dmicImportLogId -> dmic_import_log_id\n  SNOMED_ID -> snomed_id\n  code -> code\n  equivalent_ae_code -> equivalent_ae_code\n  ROWNUMBER_ID -> rownumber_id\n  PRIMARYKEY_ID -> primarykey_id\n  date -> date\n  time -> time\n  timestamp -> timestamp"
    )
}}
select
    "is_code_approved" as is_code_approved,
    "dmicImportLogId" as dmic_import_log_id,
    "SNOMED_ID" as snomed_id,
    "code" as code,
    "equivalent_ae_code" as equivalent_ae_code,
    "ROWNUMBER_ID" as rownumber_id,
    "PRIMARYKEY_ID" as primarykey_id,
    "date" as date,
    "time" as time,
    "timestamp" as timestamp
from {{ source('sus_ae', 'clinical.investigations.snomed') }}
