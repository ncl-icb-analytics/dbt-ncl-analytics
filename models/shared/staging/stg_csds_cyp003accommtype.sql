-- Staging model for csds.CYP003AccommType
-- Source: "DATA_LAKE"."CSDS"
-- Description: Community services dataset

select
    "SK" as sk,
    "LOCAL PATIENT IDENTIFIER (EXTENDED)" as local_patient_identifier_extended,
    "ACCOMMODATION STATUS CODE" as accommodation_status_code,
    "ACCOMMODATION STATUS RECORDED DATE" as accommodation_status_recorded_date,
    "EFFECTIVE FROM" as effective_from,
    "RECORD NUMBER" as record_number,
    "CYP003 UNIQUE ID" as cyp003_unique_id,
    "ORGANISATION IDENTIFIER (CODE OF PROVIDER)" as organisation_identifier_code_of_provider,
    "ORGANISATION CODE (PROVIDER)" as organisation_code_provider,
    "PERSON ID" as person_id,
    "UNIQUE CSDS ID (PATIENT)" as unique_csds_id_patient,
    "UNIQUE SUBMISSION ID" as unique_submission_id,
    "BSP UNIQUE ID" as bsp_unique_id,
    "AGE AT ACCOMMODATION STATUS DATE" as age_at_accommodation_status_date,
    "AGE AT ACCOMMODATION STATUS DATE (YEARS)" as age_at_accommodation_status_date_years,
    "IC_AGE_AT_ACCOMMODATION_STATUS_DATE" as ic_age_at_accommodation_status_date,
    "RECORD START DATE" as record_start_date,
    "RECORD END DATE" as record_end_date,
    "UNIQUE MONTH ID" as unique_month_id,
    "AGE GROUP (ACCOMMODATION STATUS DATE)" as age_group_accommodation_status_date,
    "AGE BAND (ACCOMMODATION STATUS DATE)" as age_band_accommodation_status_date,
    "dmicImportLogId" as dmic_import_log_id,
    "dmicSystemId" as dmic_system_id,
    "dmicCCGCode" as dmic_ccg_code,
    "dmicCCG" as dmic_ccg,
    "Unique_LocalPatientId" as unique_local_patient_id,
    "UniqueCYPHS_ID_Patient" as unique_cyphs_id_patient,
    "FILE TYPE" as file_type,
    "REPORTING PERIOD START DATE" as reporting_period_start_date,
    "REPORTING PERIOD END DATE" as reporting_period_end_date
from {{ source('csds', 'CYP003AccommType') }}
