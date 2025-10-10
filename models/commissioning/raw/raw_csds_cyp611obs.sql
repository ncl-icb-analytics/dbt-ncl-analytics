-- Raw layer model for csds.CYP611Obs
-- Source: "DATA_LAKE"."CSDS"
-- Description: Community services dataset
-- This is a 1:1 passthrough from source with standardized column names
select
    "SK" as sk,
    "CARE ACTIVITY IDENTIFIER" as care_activity_identifier,
    "PERSON WEIGHT" as person_weight,
    "PERSON HEIGHT IN METRES" as person_height_in_metres,
    "PERSON LENGTH IN CENTIMETRES" as person_length_in_centimetres,
    "EFFECTIVE FROM" as effective_from,
    "RECORD NUMBER" as record_number,
    "CYP611 UNIQUE ID" as cyp611_unique_id,
    "ORGANISATION IDENTIFIER (CODE OF PROVIDER)" as organisation_identifier_code_of_provider,
    "ORGANISATION CODE (PROVIDER)" as organisation_code_provider,
    "PERSON ID" as person_id,
    "UNIQUE CSDS ID (PATIENT)" as unique_csds_id_patient,
    "UNIQUE SUBMISSION ID" as unique_submission_id,
    "BSP UNIQUE ID" as bsp_unique_id,
    "UNIQUE CARE ACTIVITY IDENTIFIER" as unique_care_activity_identifier,
    "AGE AT BMI OBSERVATION (DAYS)" as age_at_bmi_observation_days,
    "AGE AT BMI OBSERVATION (YEARS)" as age_at_bmi_observation_years,
    "IC_AGE_AT_BMI_OBSERVATION" as ic_age_at_bmi_observation,
    "SCHOOL YEAR AT BMI OBSERVATION" as school_year_at_bmi_observation,
    "UNIQUE MONTH ID" as unique_month_id,
    "AGE GROUP (BMI OBSERVATION)" as age_group_bmi_observation,
    "AGE BAND (BMI OBSERVATION)" as age_band_bmi_observation,
    "dmicImportLogId" as dmic_import_log_id,
    "dmicObservationDate" as dmic_observation_date,
    "dmicSystemId" as dmic_system_id,
    "dmicCCGCode" as dmic_ccg_code,
    "dmicCCG" as dmic_ccg,
    "UniqueCYPHS_ID_Patient" as unique_cyphs_id_patient,
    "FILE TYPE" as file_type,
    "REPORTING PERIOD START DATE" as reporting_period_start_date,
    "REPORTING PERIOD END DATE" as reporting_period_end_date
from {{ source('csds', 'CYP611Obs') }}
