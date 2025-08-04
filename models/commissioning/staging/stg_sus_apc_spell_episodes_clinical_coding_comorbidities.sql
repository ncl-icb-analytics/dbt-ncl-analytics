-- Staging model for sus_apc.spell.episodes.clinical_coding.comorbidities
-- Source: "DATA_LAKE"."SUS_UNIFIED_APC"
{% if source.get('description') %}
-- Description: SUS admitted patient care episodes and procedures
{% endif %}

select
    "ROWNUMBER_ID" as rownumber_id,
    "PRIMARYKEY_ID" as primarykey_id,
    "EPISODES_ID" as episodes_id,
    "COMORBIDITIES_ID" as comorbidities_id,
    "code" as code,
    "is_data_absent" as is_data_absent,
    "data_absent_reason" as data_absent_reason,
    "dmicImportLogId" as dmicimportlogid
from {{ source('sus_apc', 'spell.episodes.clinical_coding.comorbidities') }}
