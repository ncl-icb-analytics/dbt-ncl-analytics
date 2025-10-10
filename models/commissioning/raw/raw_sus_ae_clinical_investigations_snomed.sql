-- Raw layer model for sus_ae.clinical.investigations.snomed
-- Source: "DATA_LAKE"."SUS_UNIFIED_ECDS"
-- Description: SUS emergency care attendances and activity
-- This is a 1:1 passthrough from source with standardized column names
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
