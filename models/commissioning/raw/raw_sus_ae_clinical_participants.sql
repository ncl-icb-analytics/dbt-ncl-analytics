-- Raw layer model for sus_ae.clinical.participants
-- Source: "DATA_LAKE"."SUS_UNIFIED_ECDS"
-- Description: SUS emergency care attendances and activity
-- This is a 1:1 passthrough from source with standardized column names
select
    "PARTICIPANTS_ID" as participants_id,
    "identifier" as identifier,
    "dmicImportLogId" as dmic_import_log_id,
    "clinical_responsibility_timestamp" as clinical_responsibility_timestamp,
    "issuer" as issuer,
    "tier" as tier,
    "has_discharge_responsibility" as has_discharge_responsibility,
    "ROWNUMBER_ID" as rownumber_id,
    "PRIMARYKEY_ID" as primarykey_id
from {{ source('sus_ae', 'clinical.participants') }}
