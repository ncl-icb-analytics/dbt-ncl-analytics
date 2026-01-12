{{
    config(
        description="Raw layer (Community services dataset). 1:1 passthrough with cleaned column names. \nSource: DATA_LAKE.CSDS.CYP611Obs \ndbt: source(''csds'', ''CYP611Obs'') \nColumns:\n  SK -> sk\n  CARE ACTIVITY IDENTIFIER -> care_activity_identifier\n  PERSON WEIGHT -> person_weight\n  PERSON HEIGHT IN METRES -> person_height_in_metres\n  PERSON LENGTH IN CENTIMETRES -> person_length_in_centimetres\n  EFFECTIVE FROM -> effective_from\n  RECORD NUMBER -> record_number\n  CYP611 UNIQUE ID -> cyp611_unique_id\n  ORGANISATION IDENTIFIER (CODE OF PROVIDER) -> organisation_identifier_code_of_provider\n  ORGANISATION CODE (PROVIDER) -> organisation_code_provider\n  PERSON ID -> person_id\n  UNIQUE CSDS ID (PATIENT) -> unique_csds_id_patient\n  UNIQUE SUBMISSION ID -> unique_submission_id\n  BSP UNIQUE ID -> bsp_unique_id\n  UNIQUE CARE ACTIVITY IDENTIFIER -> unique_care_activity_identifier\n  AGE AT BMI OBSERVATION (DAYS) -> age_at_bmi_observation_days\n  AGE AT BMI OBSERVATION (YEARS) -> age_at_bmi_observation_years\n  IC_AGE_AT_BMI_OBSERVATION -> ic_age_at_bmi_observation\n  SCHOOL YEAR AT BMI OBSERVATION -> school_year_at_bmi_observation\n  UNIQUE MONTH ID -> unique_month_id\n  AGE GROUP (BMI OBSERVATION) -> age_group_bmi_observation\n  AGE BAND (BMI OBSERVATION) -> age_band_bmi_observation\n  dmicImportLogId -> dmic_import_log_id\n  dmicObservationDate -> dmic_observation_date\n  dmicSystemId -> dmic_system_id\n  dmicCCGCode -> dmic_ccg_code\n  dmicCCG -> dmic_ccg\n  UniqueCYPHS_ID_Patient -> unique_cyphs_id_patient\n  FILE TYPE -> file_type\n  REPORTING PERIOD START DATE -> reporting_period_start_date\n  REPORTING PERIOD END DATE -> reporting_period_end_date"
    )
}}
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
