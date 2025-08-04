-- Staging model for sus_apc.spell.episodes.critical_care_submitted.daily_activities.high_cost_drugs
-- Source: "DATA_LAKE"."SUS_UNIFIED_APC"
{% if source.get('description') %}
-- Description: SUS admitted patient care episodes and procedures
{% endif %}

select
    "ROWNUMBER_ID" as rownumber_id,
    "PRIMARYKEY_ID" as primarykey_id,
    "EPISODES_ID" as episodes_id,
    "CRITICAL_CARE_SUBMITTED_ID" as critical_care_submitted_id,
    "DAILY_ACTIVITIES_ID" as daily_activities_id,
    "HIGH_COST_DRUGS_ID" as high_cost_drugs_id,
    "high_cost_drugs" as high_cost_drugs,
    "dmicImportLogId" as dmicimportlogid
from {{ source('sus_apc', 'spell.episodes.critical_care_submitted.daily_activities.high_cost_drugs') }}
