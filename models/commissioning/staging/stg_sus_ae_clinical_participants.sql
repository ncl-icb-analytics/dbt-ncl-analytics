-- Staging model for sus_ae.clinical.participants
-- Source: "DATA_LAKE"."SUS_UNIFIED_ECDS"
-- Description: SUS emergency care attendances and activity

select
    "PARTICIPANTS_ID" as participants_id,
    "identifier" as identifier,
    "dmicImportLogId" as dmicimportlogid,
    "clinical_responsibility_timestamp" as clinical_responsibility_timestamp,
    "issuer" as issuer,
    "tier" as tier,
    "has_discharge_responsibility" as has_discharge_responsibility,
    "ROWNUMBER_ID" as rownumber_id,
    "PRIMARYKEY_ID" as primarykey_id
from {{ source('sus_ae', 'clinical.participants') }}
