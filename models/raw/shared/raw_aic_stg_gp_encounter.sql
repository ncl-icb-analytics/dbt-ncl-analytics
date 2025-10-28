-- Raw layer model for aic.STG_GP__ENCOUNTER
-- Source: "DATA_LAKE__NCL"."AIC_DEV"
-- Description: AIC pipelines
-- This is a 1:1 passthrough from source with standardized column names
select
    "ENCOUNTER_ID" as encounter_id,
    "PATIENT_ID" as patient_id,
    "PERSON_ID" as person_id,
    "PRACTITIONER_ID" as practitioner_id,
    "APPOINTMENT_ID" as appointment_id,
    "ORGANISATION_CODE" as organisation_code,
    "ORGANISATION_NAME" as organisation_name,
    "CLINICAL_EFFECTIVE_DATE" as clinical_effective_date,
    "ENCOUNTER_SOURCE_CONCEPT_ID" as encounter_source_concept_id,
    "ENCOUNTER_SOURCE_CONCEPT_NAME" as encounter_source_concept_name,
    "AGE_AT_EVENT" as age_at_event,
    "ADMISSION_METHOD" as admission_method,
    "END_DATE" as end_date
from {{ source('aic', 'STG_GP__ENCOUNTER') }}
