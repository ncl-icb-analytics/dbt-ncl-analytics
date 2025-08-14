-- Staging model for sus_ae.commissioning.national_pricing.exclusion_reasons
-- Source: "DATA_LAKE"."SUS_UNIFIED_ECDS"
-- Description: SUS emergency care attendances and activity

select
    "ROWNUMBER_ID" as rownumber_id,
    "PRIMARYKEY_ID" as primarykey_id,
    "EXCLUSION_REASONS_ID" as exclusion_reasons_id,
    "exclusion_reasons" as exclusion_reasons,
    "dmicImportLogId" as dmicimportlogid
from {{ source('sus_ae', 'commissioning.national_pricing.exclusion_reasons') }}
