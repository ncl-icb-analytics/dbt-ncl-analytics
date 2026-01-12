{{
    config(
        description="Raw layer (Community services dataset). 1:1 passthrough with cleaned column names. \nSource: DATA_LAKE.CSDS.CYP403CPP \ndbt: source(''csds'', ''CYP403CPP'') \nColumns:\n  SK -> sk\n  LOCAL PATIENT IDENTIFIER (EXTENDED) -> local_patient_identifier_extended\n  CHILD PROTECTION PLAN REASON CODE -> child_protection_plan_reason_code\n  CHILD PROTECTION PLAN START DATE -> child_protection_plan_start_date\n  CHILD PROTECTION PLAN END DATE -> child_protection_plan_end_date\n  EFFECTIVE FROM -> effective_from\n  RECORD NUMBER -> record_number\n  CYP403 UNIQUE ID -> cyp403_unique_id\n  ORGANISATION IDENTIFIER (CODE OF PROVIDER) -> organisation_identifier_code_of_provider\n  PERSON ID -> person_id\n  UNIQUE SUBMISSION ID -> unique_submission_id\n  AGE AT CHILD PROTECTION PLAN START DATE -> age_at_child_protection_plan_start_date\n  AGE AT CHILD PROTECTION PLAN END DATE -> age_at_child_protection_plan_end_date\n  DURATION SPENT ON CHILD PROTECTION PLAN -> duration_spent_on_child_protection_plan\n  AGE AT CHILD PROTECTION PLAN START DATE (YEARS) -> age_at_child_protection_plan_start_date_years\n  AGE AT CHILD PROTECTION PLAN END DATE (YEARS) -> age_at_child_protection_plan_end_date_years\n  RECORD START DATE -> record_start_date\n  RECORD END DATE -> record_end_date\n  UNIQUE MONTH ID -> unique_month_id\n  AGE BAND (CHILD PROTECTION PLAN START DATE) -> age_band_child_protection_plan_start_date\n  AGE BAND (CHILD PROTECTION PLAN END DATE) -> age_band_child_protection_plan_end_date\n  dmicImportLogId -> dmic_import_log_id\n  dmicSystemId -> dmic_system_id\n  dmicCCGCode -> dmic_ccg_code\n  dmicCCG -> dmic_ccg\n  Unique_LocalPatientId -> unique_local_patient_id\n  UniqueCYPHS_ID_Patient -> unique_cyphs_id_patient\n  FILE TYPE -> file_type\n  REPORTING PERIOD START DATE -> reporting_period_start_date\n  REPORTING PERIOD END DATE -> reporting_period_end_date"
    )
}}
select
    "SK" as sk,
    "LOCAL PATIENT IDENTIFIER (EXTENDED)" as local_patient_identifier_extended,
    "CHILD PROTECTION PLAN REASON CODE" as child_protection_plan_reason_code,
    "CHILD PROTECTION PLAN START DATE" as child_protection_plan_start_date,
    "CHILD PROTECTION PLAN END DATE" as child_protection_plan_end_date,
    "EFFECTIVE FROM" as effective_from,
    "RECORD NUMBER" as record_number,
    "CYP403 UNIQUE ID" as cyp403_unique_id,
    "ORGANISATION IDENTIFIER (CODE OF PROVIDER)" as organisation_identifier_code_of_provider,
    "PERSON ID" as person_id,
    "UNIQUE SUBMISSION ID" as unique_submission_id,
    "AGE AT CHILD PROTECTION PLAN START DATE" as age_at_child_protection_plan_start_date,
    "AGE AT CHILD PROTECTION PLAN END DATE" as age_at_child_protection_plan_end_date,
    "DURATION SPENT ON CHILD PROTECTION PLAN" as duration_spent_on_child_protection_plan,
    "AGE AT CHILD PROTECTION PLAN START DATE (YEARS)" as age_at_child_protection_plan_start_date_years,
    "AGE AT CHILD PROTECTION PLAN END DATE (YEARS)" as age_at_child_protection_plan_end_date_years,
    "RECORD START DATE" as record_start_date,
    "RECORD END DATE" as record_end_date,
    "UNIQUE MONTH ID" as unique_month_id,
    "AGE BAND (CHILD PROTECTION PLAN START DATE)" as age_band_child_protection_plan_start_date,
    "AGE BAND (CHILD PROTECTION PLAN END DATE)" as age_band_child_protection_plan_end_date,
    "dmicImportLogId" as dmic_import_log_id,
    "dmicSystemId" as dmic_system_id,
    "dmicCCGCode" as dmic_ccg_code,
    "dmicCCG" as dmic_ccg,
    "Unique_LocalPatientId" as unique_local_patient_id,
    "UniqueCYPHS_ID_Patient" as unique_cyphs_id_patient,
    "FILE TYPE" as file_type,
    "REPORTING PERIOD START DATE" as reporting_period_start_date,
    "REPORTING PERIOD END DATE" as reporting_period_end_date
from {{ source('csds', 'CYP403CPP') }}
