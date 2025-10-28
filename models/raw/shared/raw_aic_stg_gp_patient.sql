-- Raw layer model for aic.STG_GP__PATIENT
-- Source: "DATA_LAKE__NCL"."AIC_DEV"
-- Description: AIC pipelines
-- This is a 1:1 passthrough from source with standardized column names
select
    "PATIENT_ID" as patient_id,
    "SK_PATIENT_ID" as sk_patient_id,
    "ORGANISATION_CODE" as organisation_code,
    "ORGANISATION_NAME" as organisation_name,
    "GENDER_CONCEPT_ID" as gender_concept_id,
    "GENDER_CONCEPT_CODE" as gender_concept_code,
    "GENDER_CONCEPT_NAME" as gender_concept_name,
    "GENDER_CONCEPT_VOCABULARY" as gender_concept_vocabulary,
    "BIRTH_YEAR" as birth_year,
    "BIRTH_MONTH" as birth_month,
    "DEATH_YEAR" as death_year,
    "DEATH_MONTH" as death_month,
    "IS_DEAD" as is_dead,
    "IS_SPINE_SENSITIVE" as is_spine_sensitive,
    "IS_CONFIDENTIAL" as is_confidential,
    "IS_DUMMY_PATIENT" as is_dummy_patient
from {{ source('aic', 'STG_GP__PATIENT') }}
