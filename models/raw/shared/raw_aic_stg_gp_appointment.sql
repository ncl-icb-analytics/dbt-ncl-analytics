-- Raw layer model for aic.STG_GP__APPOINTMENT
-- Source: "DATA_LAKE__NCL"."AIC_DEV"
-- Description: AIC pipelines
-- This is a 1:1 passthrough from source with standardized column names
select
    "APPOINTMENT_ID" as appointment_id,
    "PATIENT_ID" as patient_id,
    "PERSON_ID" as person_id,
    "PRACTITIONER_ID" as practitioner_id,
    "ORGANISATION_CODE" as organisation_code,
    "ORGANISATION_NAME" as organisation_name,
    "START_DATE" as start_date,
    "PLANNED_DURATION" as planned_duration,
    "ACTUAL_DURATION" as actual_duration,
    "APPOINTMENT_STATUS_CONCEPT_ID" as appointment_status_concept_id,
    "APPOINTMENT_STATUS_CONCEPT_NAME" as appointment_status_concept_name,
    "PATIENT_WAIT" as patient_wait,
    "PATIENT_DELAY" as patient_delay,
    "DATE_TIME_SENT_IN" as date_time_sent_in,
    "DATE_TIME_LEFT" as date_time_left,
    "CANCELLED_DATE" as cancelled_date
from {{ source('aic', 'STG_GP__APPOINTMENT') }}
