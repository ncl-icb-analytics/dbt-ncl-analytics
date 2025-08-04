-- Staging model for sus_apc.spell.commissioning.tariff_calculation.exclusion_reasons
-- Source: "DATA_LAKE"."SUS_UNIFIED_APC"
{% if source.get('description') %}
-- Description: SUS admitted patient care episodes and procedures
{% endif %}

select
    "ROWNUMBER_ID" as rownumber_id,
    "PRIMARYKEY_ID" as primarykey_id,
    "EXCLUSION_REASONS_ID" as exclusion_reasons_id,
    "exclusion_reasons" as exclusion_reasons,
    "dmicImportLogId" as dmicimportlogid
from {{ source('sus_apc', 'spell.commissioning.tariff_calculation.exclusion_reasons') }}
