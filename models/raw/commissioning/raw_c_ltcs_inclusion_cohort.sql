-- Raw layer model for c_ltcs.INCLUSION_COHORT
-- Source: "DEV__PUBLISHED_REPORTING__DIRECT_CARE"."C_LTCS"
-- Description: C-LTCS tables
-- This is a 1:1 passthrough from source with standardized column names
select
    "PATIENT_ID" as patient_id,
    "RE_ID_KEY" as re_id_key,
    "OLIDS_ID" as olids_id,
    "PRACTICE_CODE" as practice_code,
    "PRACTICE_NAME" as practice_name,
    "PCN_CODE" as pcn_code,
    "PCN_NAME" as pcn_name,
    "MAIN_LANGUAGE" as main_language,
    "AGE" as age,
    "GENDER" as gender,
    "ETHNICITY_CATEGORY" as ethnicity_category,
    "ELIGIBLE" as eligible
from {{ source('c_ltcs', 'INCLUSION_COHORT') }}
