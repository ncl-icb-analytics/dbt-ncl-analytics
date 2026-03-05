{{
    config(
        description="Raw layer (Analyst-managed reference datasets and business rules). 1:1 passthrough with cleaned column names. \nSource: DATA_LAKE__NCL.ANALYST_MANAGED.NMUH_200_PATIENT_SAMPLE \ndbt: source(''reference_analyst_managed'', ''NMUH_200_PATIENT_SAMPLE'') \nColumns:\n  PATIENT_SER -> patient_ser\n  PATIENT_NUMBER -> patient_number\n  APPOINTMENT_DATE -> appointment_date\n  TUMOUR_GROUP -> tumour_group\n  TREATMENT_SITE -> treatment_site\n  DAILY_TREATMENT_ACTIVITY -> daily_treatment_activity\n  TREATMENT_TECHNIQUE -> treatment_technique\n  TREATMENT_MACHINE -> treatment_machine\n  APPOINTMENT_TIME_START_TIME -> appointment_time_start_time\n  APPOINTMENT_TIME_END_TIME -> appointment_time_end_time\n  FRACTION_TIME_MINS -> fraction_time_mins\n  DIAGNOSIS_SEQ_CODE_DESCRIPTION -> diagnosis_seq_code_description\n  INTENDEND_NUMBER_OF_FRACTIONS -> intendend_number_of_fractions\n  COURSE_ID -> course_id"
    )
}}
select
    "PATIENT_SER" as patient_ser,
    "PATIENT_NUMBER" as patient_number,
    "APPOINTMENT_DATE" as appointment_date,
    "TUMOUR_GROUP" as tumour_group,
    "TREATMENT_SITE" as treatment_site,
    "DAILY_TREATMENT_ACTIVITY" as daily_treatment_activity,
    "TREATMENT_TECHNIQUE" as treatment_technique,
    "TREATMENT_MACHINE" as treatment_machine,
    "APPOINTMENT_TIME_START_TIME" as appointment_time_start_time,
    "APPOINTMENT_TIME_END_TIME" as appointment_time_end_time,
    "FRACTION_TIME_MINS" as fraction_time_mins,
    "DIAGNOSIS_SEQ_CODE_DESCRIPTION" as diagnosis_seq_code_description,
    "INTENDEND_NUMBER_OF_FRACTIONS" as intendend_number_of_fractions,
    "COURSE_ID" as course_id
from {{ source('reference_analyst_managed', 'NMUH_200_PATIENT_SAMPLE') }}
