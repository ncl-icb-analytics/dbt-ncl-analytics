-- Raw layer model for aic.STG_SUS__ECDS_ATTENDANCE
-- Source: "DATA_LAKE__NCL"."AIC_DEV"
-- Description: AIC pipelines
-- This is a 1:1 passthrough from source with standardized column names
select
    "ECDS_ATTENDANCE_ID" as ecds_attendance_id,
    "SK_PATIENT_ID" as sk_patient_id,
    "AGE_AT_ARRIVAL" as age_at_arrival,
    "ECDS_ATTENDANCE_PROVIDER_CODE" as ecds_attendance_provider_code,
    "ECDS_ATTENDANCE_PROVIDER_NAME" as ecds_attendance_provider_name,
    "ECDS_ATTENDANCE_SITE_CODE" as ecds_attendance_site_code,
    "ECDS_ATTENDANCE_SITE_NAME" as ecds_attendance_site_name,
    "ECDS_ATTENDANCE_DEPARTMENT_TYPE" as ecds_attendance_department_type,
    "ECDS_ATTENDANCE_ARRIVAL_DATE" as ecds_attendance_arrival_date,
    "ECDS_ATTENDANCE_ARRIVAL_TIME" as ecds_attendance_arrival_time,
    "ECDS_ATTENDANCE_ARRIVAL_MODE_CODE" as ecds_attendance_arrival_mode_code,
    "ECDS_ATTENDANCE_ARRIVAL_MODE_NAME" as ecds_attendance_arrival_mode_name,
    "ECDS_ATTENDANCE_ARRIVAL_CATEGORY" as ecds_attendance_arrival_category,
    "ECDS_ATTENDANCE_DEPARTURE_DATE" as ecds_attendance_departure_date,
    "ECDS_ATTENDANCE_DEPARTURE_TIME" as ecds_attendance_departure_time,
    "ECDS_ATTENDANCE_DISCHARGE_STATUS_CODE" as ecds_attendance_discharge_status_code,
    "ECDS_ATTENDANCE_DISCHARGE_STATUS_NAME" as ecds_attendance_discharge_status_name,
    "ECDS_ATTENDANCE_DISCHARGE_DESTINATION_CODE" as ecds_attendance_discharge_destination_code,
    "ECDS_ATTENDANCE_DISCHARGE_DESTINATION_NAME" as ecds_attendance_discharge_destination_name,
    "ECDS_ATTENDANCE_DISCHARGE_FOLLOW_UP_CODE" as ecds_attendance_discharge_follow_up_code,
    "ECDS_ATTENDANCE_DISCHARGE_FOLLOW_UP_NAME" as ecds_attendance_discharge_follow_up_name,
    "ECDS_ATTENDANCE_DECISION_TO_ADMIT_DATE" as ecds_attendance_decision_to_admit_date,
    "ECDS_ATTENDANCE_DECISION_TO_ADMIT_TIME" as ecds_attendance_decision_to_admit_time,
    "ECDS_ATTENDANCE_DECISION_TO_ADMIT_TREATMENT_FUNCTION_CODE" as ecds_attendance_decision_to_admit_treatment_function_code,
    "ECDS_ATTENDANCE_DECISION_TO_ADMIT_TREATMENT_FUNCTION_NAME" as ecds_attendance_decision_to_admit_treatment_function_name,
    "ECDS_ATTENDANCE_DECISION_TO_ADMIT_RECEIVING_SITE" as ecds_attendance_decision_to_admit_receiving_site,
    "COMMISSIONING_HRG_CODE" as commissioning_hrg_code,
    "COMMISSIONING_TARIFF" as commissioning_tariff,
    "COMMISSIONING_FINAL_PRICE" as commissioning_final_price
from {{ source('aic', 'STG_SUS__ECDS_ATTENDANCE') }}
