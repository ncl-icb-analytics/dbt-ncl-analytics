-- Raw layer model for sus_ae.clinical.comorbidities
-- Source: "DATA_LAKE"."SUS_UNIFIED_ECDS"
-- Description: SUS emergency care attendances and activity
-- This is a 1:1 passthrough from source with standardized column names
select
    "ROWNUMBER_ID" as rownumber_id,
    "PRIMARYKEY_ID" as primarykey_id,
    "COMORBIDITIES_ID" as comorbidities_id,
    "code" as code,
    "is_code_approved" as is_code_approved,
    "dmicImportLogId" as dmic_import_log_id
from {{ source('sus_ae', 'clinical.comorbidities') }}
