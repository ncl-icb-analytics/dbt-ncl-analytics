-- Raw layer model for aic.STG_SUS__OP_APPOINTMENT
-- Source: "DATA_LAKE__NCL"."AIC_DEV"
-- Description: AIC pipelines
-- This is a 1:1 passthrough from source with standardized column names
select
    "OP_APPOINTMENT_ID" as op_appointment_id,
    "SK_PATIENT_ID" as sk_patient_id,
    "OP_APPOINTMENT_DATE" as op_appointment_date,
    "OP_APPOINTMENT_TIME" as op_appointment_time,
    "OP_APPOINTMENT_ATTENDED_CODE" as op_appointment_attended_code,
    "OP_APPOINTMENT_ATTENDED_NAME" as op_appointment_attended_name,
    "OP_APPOINTMENT_SPECIALTY_CODE" as op_appointment_specialty_code,
    "OP_APPOINTMENT_SPECIALTY_NAME" as op_appointment_specialty_name,
    "OP_APPOINTMENT_OUTCOME_CODE" as op_appointment_outcome_code,
    "OP_APPOINTMENT_OUTCOME_NAME" as op_appointment_outcome_name,
    "OP_APPOINTMENT_ADMIN_CATEGORY_CODE" as op_appointment_admin_category_code,
    "OP_APPOINTMENT_ADMIN_CATEGORY_NAME" as op_appointment_admin_category_name,
    "OP_APPOINTMENT_TARIFF_FINAL_PRICE" as op_appointment_tariff_final_price
from {{ source('aic', 'STG_SUS__OP_APPOINTMENT') }}
