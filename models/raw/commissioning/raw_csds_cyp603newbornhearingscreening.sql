{{
    config(
        description="Raw layer (Community services dataset). 1:1 passthrough with cleaned column names. \nSource: DATA_LAKE.CSDS.CYP603NewbornHearingScreening \ndbt: source(''csds'', ''CYP603NewbornHearingScreening'') \nColumns:\n  SK -> sk\n  LOCAL PATIENT IDENTIFIER (EXTENDED) -> local_patient_identifier_extended\n  NEWBORN HEARING SCREENING OUTCOME -> newborn_hearing_screening_outcome\n  SERVICE REQUEST DATE (NEWBORN HEARING AUDIOLOGY) -> service_request_date_newborn_hearing_audiology\n  PROCEDURE DATE (NEWBORN HEARING AUDIOLOGY) -> procedure_date_newborn_hearing_audiology\n  NEWBORN HEARING AUDIOLOGY OUTCOME -> newborn_hearing_audiology_outcome\n  EFFECTIVE FROM -> effective_from\n  RECORD NUMBER -> record_number\n  CYP603 UNIQUE ID -> cyp603_unique_id\n  ORGANISATION IDENTIFIER (CODE OF PROVIDER) -> organisation_identifier_code_of_provider\n  ORGANISATION CODE (PROVIDER) -> organisation_code_provider\n  PERSON ID -> person_id\n  UNIQUE CSDS ID (PATIENT) -> unique_csds_id_patient\n  UNIQUE SUBMISSION ID -> unique_submission_id\n  BSP UNIQUE ID -> bsp_unique_id\n  AGE AT NEWBORN HEARING SERVICE REQUEST DATE -> age_at_newborn_hearing_service_request_date\n  IC_AGE_AT_NEWBORN_HEARING_SERVICE_REQUEST_DATE -> ic_age_at_newborn_hearing_service_request_date\n  AGE AT NEWBORN HEARING PROCEDURE DATE -> age_at_newborn_hearing_procedure_date\n  IC_AGE_AT_NEWBORN_HEARING_PROCEDURE_DATE -> ic_age_at_newborn_hearing_procedure_date\n  RECORD START DATE -> record_start_date\n  RECORD END DATE -> record_end_date\n  UNIQUE MONTH ID -> unique_month_id\n  dmicImportLogId -> dmic_import_log_id\n  dmicSystemId -> dmic_system_id\n  dmicCCGCode -> dmic_ccg_code\n  dmicCCG -> dmic_ccg\n  Unique_LocalPatientId -> unique_local_patient_id\n  UniqueCYPHS_ID_Patient -> unique_cyphs_id_patient\n  FILE TYPE -> file_type\n  REPORTING PERIOD START DATE -> reporting_period_start_date\n  REPORTING PERIOD END DATE -> reporting_period_end_date"
    )
}}
select
    "SK" as sk,
    "LOCAL PATIENT IDENTIFIER (EXTENDED)" as local_patient_identifier_extended,
    "NEWBORN HEARING SCREENING OUTCOME" as newborn_hearing_screening_outcome,
    "SERVICE REQUEST DATE (NEWBORN HEARING AUDIOLOGY)" as service_request_date_newborn_hearing_audiology,
    "PROCEDURE DATE (NEWBORN HEARING AUDIOLOGY)" as procedure_date_newborn_hearing_audiology,
    "NEWBORN HEARING AUDIOLOGY OUTCOME" as newborn_hearing_audiology_outcome,
    "EFFECTIVE FROM" as effective_from,
    "RECORD NUMBER" as record_number,
    "CYP603 UNIQUE ID" as cyp603_unique_id,
    "ORGANISATION IDENTIFIER (CODE OF PROVIDER)" as organisation_identifier_code_of_provider,
    "ORGANISATION CODE (PROVIDER)" as organisation_code_provider,
    "PERSON ID" as person_id,
    "UNIQUE CSDS ID (PATIENT)" as unique_csds_id_patient,
    "UNIQUE SUBMISSION ID" as unique_submission_id,
    "BSP UNIQUE ID" as bsp_unique_id,
    "AGE AT NEWBORN HEARING SERVICE REQUEST DATE" as age_at_newborn_hearing_service_request_date,
    "IC_AGE_AT_NEWBORN_HEARING_SERVICE_REQUEST_DATE" as ic_age_at_newborn_hearing_service_request_date,
    "AGE AT NEWBORN HEARING PROCEDURE DATE" as age_at_newborn_hearing_procedure_date,
    "IC_AGE_AT_NEWBORN_HEARING_PROCEDURE_DATE" as ic_age_at_newborn_hearing_procedure_date,
    "RECORD START DATE" as record_start_date,
    "RECORD END DATE" as record_end_date,
    "UNIQUE MONTH ID" as unique_month_id,
    "dmicImportLogId" as dmic_import_log_id,
    "dmicSystemId" as dmic_system_id,
    "dmicCCGCode" as dmic_ccg_code,
    "dmicCCG" as dmic_ccg,
    "Unique_LocalPatientId" as unique_local_patient_id,
    "UniqueCYPHS_ID_Patient" as unique_cyphs_id_patient,
    "FILE TYPE" as file_type,
    "REPORTING PERIOD START DATE" as reporting_period_start_date,
    "REPORTING PERIOD END DATE" as reporting_period_end_date
from {{ source('csds', 'CYP603NewbornHearingScreening') }}
