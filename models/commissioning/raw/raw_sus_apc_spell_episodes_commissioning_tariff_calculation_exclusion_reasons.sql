-- Raw layer model for sus_apc.spell.episodes.commissioning.tariff_calculation.exclusion_reasons
-- Source: "DATA_LAKE"."SUS_UNIFIED_APC"
-- Description: SUS admitted patient care episodes and procedures
-- This is a 1:1 passthrough from source with standardized column names
select
    "ROWNUMBER_ID" as rownumber_id,
    "PRIMARYKEY_ID" as primarykey_id,
    "EPISODES_ID" as episodes_id,
    "EXCLUSION_REASONS_ID" as exclusion_reasons_id,
    "exclusion_reasons" as exclusion_reasons,
    "dmicImportLogId" as dmic_import_log_id
from {{ source('sus_apc', 'spell.episodes.commissioning.tariff_calculation.exclusion_reasons') }}
