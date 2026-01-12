{{
    config(
        description="Raw layer (Community services dataset). 1:1 passthrough with cleaned column names. \nSource: DATA_LAKE.CSDS.CYP610BreastfeedingStatus \ndbt: source(''csds'', ''CYP610BreastfeedingStatus'') \nColumns:\n  SK -> sk\n  CARE ACTIVITY IDENTIFIER -> care_activity_identifier\n  BREASTFEEDING STATUS -> breastfeeding_status\n  EFFECTIVE FROM -> effective_from\n  RECORD NUMBER -> record_number\n  CYP610 UNIQUE ID -> cyp610_unique_id\n  ORGANISATION IDENTIFIER (CODE OF PROVIDER) -> organisation_identifier_code_of_provider\n  ORGANISATION CODE (PROVIDER) -> organisation_code_provider\n  PERSON ID -> person_id\n  UNIQUE CSDS ID (PATIENT) -> unique_csds_id_patient\n  UNIQUE SUBMISSION ID -> unique_submission_id\n  BSP UNIQUE ID -> bsp_unique_id\n  UNIQUE CARE ACTIVITY IDENTIFIER -> unique_care_activity_identifier\n  AGE AT BREASTFEEDING STATUS OBSERVATION DATE (DAYS) -> age_at_breastfeeding_status_observation_date_days\n  AGE AT BREASTFEEDING STATUS OBSERVATION DATE (YEARS) -> age_at_breastfeeding_status_observation_date_years\n  IC_AGE_BREASTFEEDING_STATUS_OBSERVATION_DATE -> ic_age_breastfeeding_status_observation_date\n  UNIQUE MONTH ID -> unique_month_id\n  AGE BAND (BREASTFEEDING STATUS DATE) -> age_band_breastfeeding_status_date\n  dmicImportLogId -> dmic_import_log_id\n  dmicObservationDate -> dmic_observation_date\n  dmicSystemId -> dmic_system_id\n  dmicCCGCode -> dmic_ccg_code\n  dmicCCG -> dmic_ccg\n  UniqueCYPHS_ID_Patient -> unique_cyphs_id_patient\n  FILE TYPE -> file_type\n  REPORTING PERIOD START DATE -> reporting_period_start_date\n  REPORTING PERIOD END DATE -> reporting_period_end_date"
    )
}}
select
    "SK" as sk,
    "CARE ACTIVITY IDENTIFIER" as care_activity_identifier,
    "BREASTFEEDING STATUS" as breastfeeding_status,
    "EFFECTIVE FROM" as effective_from,
    "RECORD NUMBER" as record_number,
    "CYP610 UNIQUE ID" as cyp610_unique_id,
    "ORGANISATION IDENTIFIER (CODE OF PROVIDER)" as organisation_identifier_code_of_provider,
    "ORGANISATION CODE (PROVIDER)" as organisation_code_provider,
    "PERSON ID" as person_id,
    "UNIQUE CSDS ID (PATIENT)" as unique_csds_id_patient,
    "UNIQUE SUBMISSION ID" as unique_submission_id,
    "BSP UNIQUE ID" as bsp_unique_id,
    "UNIQUE CARE ACTIVITY IDENTIFIER" as unique_care_activity_identifier,
    "AGE AT BREASTFEEDING STATUS OBSERVATION DATE (DAYS)" as age_at_breastfeeding_status_observation_date_days,
    "AGE AT BREASTFEEDING STATUS OBSERVATION DATE (YEARS)" as age_at_breastfeeding_status_observation_date_years,
    "IC_AGE_BREASTFEEDING_STATUS_OBSERVATION_DATE" as ic_age_breastfeeding_status_observation_date,
    "UNIQUE MONTH ID" as unique_month_id,
    "AGE BAND (BREASTFEEDING STATUS DATE)" as age_band_breastfeeding_status_date,
    "dmicImportLogId" as dmic_import_log_id,
    "dmicObservationDate" as dmic_observation_date,
    "dmicSystemId" as dmic_system_id,
    "dmicCCGCode" as dmic_ccg_code,
    "dmicCCG" as dmic_ccg,
    "UniqueCYPHS_ID_Patient" as unique_cyphs_id_patient,
    "FILE TYPE" as file_type,
    "REPORTING PERIOD START DATE" as reporting_period_start_date,
    "REPORTING PERIOD END DATE" as reporting_period_end_date
from {{ source('csds', 'CYP610BreastfeedingStatus') }}
