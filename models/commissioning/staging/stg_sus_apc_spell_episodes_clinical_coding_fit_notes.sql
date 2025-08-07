-- Staging model for sus_apc.spell.episodes.clinical_coding.fit_notes
-- Source: "DATA_LAKE"."SUS_UNIFIED_APC"
-- Description: SUS admitted patient care episodes and procedures

select
    "ROWNUMBER_ID" as rownumber_id,
    "PRIMARYKEY_ID" as primarykey_id,
    "EPISODES_ID" as episodes_id,
    "FIT_NOTES_ID" as fit_notes_id,
    "condition" as condition,
    "assessment_date" as assessment_date,
    "diagnosis" as diagnosis,
    "start_date" as start_date,
    "end_date" as end_date,
    "duration" as duration,
    "recorded_date" as recorded_date,
    "follow_up_assessment_required_indicator" as follow_up_assessment_required_indicator,
    "issuer" as issuer,
    "dmicImportLogId" as dmicimportlogid
from {{ source('sus_apc', 'spell.episodes.clinical_coding.fit_notes') }}
