-- Raw layer model for aic.STG_GP__PATIENT_REGISTRATION
-- Source: "DATA_LAKE__NCL"."AIC_DEV"
-- Description: AIC pipelines
-- This is a 1:1 passthrough from source with standardized column names
select
    "PATIENT_REGISTERED_ID" as patient_registered_id,
    "PERSON_ID" as person_id,
    "PATIENT_ID" as patient_id,
    "ORGANISATION_CODE" as organisation_code,
    "ORGANISATION_NAME" as organisation_name,
    "START_DATE" as start_date,
    "END_DATE" as end_date,
    "REGISTRATION_DAYS" as registration_days,
    "IS_ACTIVE_REGISTRATION" as is_active_registration,
    "IS_REGISTRATION_ENDED" as is_registration_ended,
    "DQ_IS_BAD_DATES" as dq_is_bad_dates,
    "DQ_IS_MULTIPLE_ACTIVE" as dq_is_multiple_active
from {{ source('aic', 'STG_GP__PATIENT_REGISTRATION') }}
