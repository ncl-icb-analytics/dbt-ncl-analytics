{{
    config(
        description="Raw layer (SUS emergency care attendances and activity). 1:1 passthrough with cleaned column names. \nSource: DATA_LAKE.SUS_UNIFIED_ECDS.clinical.coded_observations \ndbt: source(''sus_ae'', ''clinical.coded_observations'') \nColumns:\n  is_code_approved -> is_code_approved\n  value -> value\n  code -> code\n  CODED_OBSERVATIONS_ID -> coded_observations_id\n  timestamp -> timestamp\n  dmicImportLogId -> dmic_import_log_id\n  ucum_unit_of_measurement -> ucum_unit_of_measurement\n  ROWNUMBER_ID -> rownumber_id\n  PRIMARYKEY_ID -> primarykey_id"
    )
}}
select
    "is_code_approved" as is_code_approved,
    "value" as value,
    "code" as code,
    "CODED_OBSERVATIONS_ID" as coded_observations_id,
    "timestamp" as timestamp,
    "dmicImportLogId" as dmic_import_log_id,
    "ucum_unit_of_measurement" as ucum_unit_of_measurement,
    "ROWNUMBER_ID" as rownumber_id,
    "PRIMARYKEY_ID" as primarykey_id
from {{ source('sus_ae', 'clinical.coded_observations') }}
