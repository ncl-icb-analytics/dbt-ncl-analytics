-- Raw layer model for aic.INT_VISIT_OCCURRENCE
-- Source: "DATA_LAKE__NCL"."AIC_DEV"
-- Description: AIC pipelines
-- This is a 1:1 passthrough from source with standardized column names
select
    "VISIT_OCCURRENCE_ID" as visit_occurrence_id,
    "PERSON_ID" as person_id,
    "VISIT_SOURCE_TYPE" as visit_source_type,
    "VISIT_SOURCE_ID" as visit_source_id,
    "VISIT_START_DATE" as visit_start_date,
    "VISIT_START_TIME" as visit_start_time,
    "VISIT_END_DATE" as visit_end_date,
    "VISIT_END_TIME" as visit_end_time,
    "VISIT_LENGTH_OF_STAY_HOURS" as visit_length_of_stay_hours,
    "VISIT_LENGTH_OF_STAY_DAYS" as visit_length_of_stay_days,
    "ORGANISATION_CODE" as organisation_code,
    "ORGANISATION_NAME" as organisation_name,
    "SITE_CODE" as site_code,
    "SITE_NAME" as site_name,
    "VISIT_ADMISSION_METHOD" as visit_admission_method,
    "VISIT_ADMISSION_SOURCE" as visit_admission_source,
    "VISIT_SPECIALTY" as visit_specialty,
    "VISIT_SOURCE_SUBTYPE" as visit_source_subtype,
    "VISIT_DISCHARGE_DESTINATION" as visit_discharge_destination,
    "VISIT_DISCHARGE_METHOD" as visit_discharge_method,
    "COMMISSIONING_FINAL_PRICE" as commissioning_final_price,
    "AGE_AT_EVENT" as age_at_event,
    "IS_LESS_THAN_24_HOURS" as is_less_than_24_hours,
    "IS_GREATER_THAN_3_MONTHS" as is_greater_than_3_months,
    "IS_GREATER_THAN_6_MONTHS" as is_greater_than_6_months,
    "IS_GREATER_THAN_12_MONTHS" as is_greater_than_12_months
from {{ source('aic', 'INT_VISIT_OCCURRENCE') }}
