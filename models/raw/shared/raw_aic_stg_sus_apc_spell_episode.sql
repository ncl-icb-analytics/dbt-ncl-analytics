-- Raw layer model for aic.STG_SUS__APC_SPELL_EPISODE
-- Source: "DATA_LAKE__NCL"."AIC_DEV"
-- Description: AIC pipelines
-- This is a 1:1 passthrough from source with standardized column names
select
    "EPISODE_ID" as episode_id,
    "SK_PATIENT_ID" as sk_patient_id,
    "SPELL_ID" as spell_id,
    "BIRTH_DATE" as birth_date,
    "BIRTH_MONTH" as birth_month,
    "BIRTH_YEAR" as birth_year,
    "AGE_ON_ADMISSION" as age_on_admission,
    "EPISODE_NUMBER" as episode_number,
    "EPISODE_START_DATE" as episode_start_date,
    "EPISODE_START_TIME" as episode_start_time,
    "EPISODE_END_DATE" as episode_end_date,
    "EPISODE_END_TIME" as episode_end_time,
    "EPISODE_MAIN_SPECIALTY" as episode_main_specialty,
    "EPISODE_TREATMENT_FUNCTION" as episode_treatment_function
from {{ source('aic', 'STG_SUS__APC_SPELL_EPISODE') }}
