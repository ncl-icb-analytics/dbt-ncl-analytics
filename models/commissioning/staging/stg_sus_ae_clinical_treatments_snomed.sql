-- Staging model for sus_ae.clinical.treatments.snomed
-- Source: "DATA_LAKE"."SUS_UNIFIED_ECDS"
-- Description: SUS emergency care attendances and activity

select
    "ROWNUMBER_ID" as rownumber_id,
    "PRIMARYKEY_ID" as primarykey_id,
    "equivalent_ae_code" as equivalent_ae_code,
    "time" as time,
    "dmicImportLogId" as dmicimportlogid,
    "is_code_approved" as is_code_approved,
    "SNOMED_ID" as snomed_id,
    "date" as date,
    "timestamp" as timestamp,
    "code" as code
from {{ source('sus_ae', 'clinical.treatments.snomed') }}
