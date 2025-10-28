-- Raw layer model for aic.STG_SUS__APC_SPELL
-- Source: "DATA_LAKE__NCL"."AIC_DEV"
-- Description: AIC pipelines
-- This is a 1:1 passthrough from source with standardized column names
select
    "SPELL_ID" as spell_id,
    "SK_PATIENT_ID" as sk_patient_id,
    "AGE_ON_ADMISSION" as age_on_admission,
    "SPELL_ADMISSION_METHOD" as spell_admission_method,
    "SPELL_ADMISSION_SOURCE" as spell_admission_source,
    "SPELL_ADMISSION_DATE" as spell_admission_date,
    "SPELL_ADMISSION_TIME" as spell_admission_time,
    "SPELL_ADMISSION_ADMISSION_TYPE" as spell_admission_admission_type,
    "SPELL_ADMISSION_ADMISSION_SUB_TYPE" as spell_admission_admission_sub_type,
    "SPELL_DISCHARGE_DESTINATION" as spell_discharge_destination,
    "SPELL_DISCHARGE_METHOD" as spell_discharge_method,
    "SPELL_DISCHARGE_DATE" as spell_discharge_date,
    "SPELL_DISCHARGE_TIME" as spell_discharge_time,
    "SPELL_DISCHARGE_LENGTH_OF_HOSPITAL_STAY" as spell_discharge_length_of_hospital_stay,
    "COMMISSIONING_TARIFF_CALCULATION_TARIFF_APPLIED" as commissioning_tariff_calculation_tariff_applied,
    "COMMISSIONING_TARIFF_CALCULATION_FINAL_PRICE" as commissioning_tariff_calculation_final_price
from {{ source('aic', 'STG_SUS__APC_SPELL') }}
