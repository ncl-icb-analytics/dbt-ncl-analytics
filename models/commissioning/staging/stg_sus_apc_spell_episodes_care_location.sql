-- Staging model for sus_apc.spell.episodes.care_location
-- Source: "DATA_LAKE"."SUS_UNIFIED_APC"
-- Description: SUS admitted patient care episodes and procedures

select
    "end_date" as end_date,
    "security_level" as security_level,
    "start_time" as start_time,
    "CARE_LOCATION_ID" as care_location_id,
    "site_code_of_treatment" as site_code_of_treatment,
    "class" as class,
    "type" as type,
    "ROWNUMBER_ID" as rownumber_id,
    "PRIMARYKEY_ID" as primarykey_id,
    "EPISODES_ID" as episodes_id,
    "night_period_availability" as night_period_availability,
    "start_date" as start_date,
    "end_time" as end_time,
    "dmicImportLogId" as dmicimportlogid,
    "stage" as stage,
    "ward_stay_sequence_number" as ward_stay_sequence_number,
    "ward_code" as ward_code,
    "intended_care_intensity" as intended_care_intensity,
    "intended_age_group" as intended_age_group,
    "sex_of_patients" as sex_of_patients,
    "day_period_availability" as day_period_availability
from {{ source('sus_apc', 'spell.episodes.care_location') }}
