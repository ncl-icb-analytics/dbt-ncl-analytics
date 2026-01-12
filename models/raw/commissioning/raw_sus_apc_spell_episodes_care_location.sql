{{
    config(
        description="Raw layer (SUS admitted patient care episodes and procedures). 1:1 passthrough with cleaned column names. \nSource: DATA_LAKE.SUS_UNIFIED_APC.spell.episodes.care_location \ndbt: source(''sus_apc'', ''spell.episodes.care_location'') \nColumns:\n  end_date -> end_date\n  security_level -> security_level\n  start_time -> start_time\n  CARE_LOCATION_ID -> care_location_id\n  site_code_of_treatment -> site_code_of_treatment\n  class -> class\n  type -> type\n  ROWNUMBER_ID -> rownumber_id\n  PRIMARYKEY_ID -> primarykey_id\n  EPISODES_ID -> episodes_id\n  night_period_availability -> night_period_availability\n  start_date -> start_date\n  end_time -> end_time\n  dmicImportLogId -> dmic_import_log_id\n  stage -> stage\n  ward_stay_sequence_number -> ward_stay_sequence_number\n  ward_code -> ward_code\n  intended_care_intensity -> intended_care_intensity\n  intended_age_group -> intended_age_group\n  sex_of_patients -> sex_of_patients\n  day_period_availability -> day_period_availability"
    )
}}
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
    "dmicImportLogId" as dmic_import_log_id,
    "stage" as stage,
    "ward_stay_sequence_number" as ward_stay_sequence_number,
    "ward_code" as ward_code,
    "intended_care_intensity" as intended_care_intensity,
    "intended_age_group" as intended_age_group,
    "sex_of_patients" as sex_of_patients,
    "day_period_availability" as day_period_availability
from {{ source('sus_apc', 'spell.episodes.care_location') }}
