{{
    config(
        description="Raw layer (Community services dataset). 1:1 passthrough with cleaned column names. \nSource: DATA_LAKE.CSDS.CYP004CarePlanType \ndbt: source(''csds'', ''CYP004CarePlanType'') \nColumns:\n  SK -> sk\n  CARE PLAN IDENTIFIER -> care_plan_identifier\n  LOCAL PATIENT IDENTIFIER (EXTENDED) -> local_patient_identifier_extended\n  CARE PLAN TYPE (COMMUNITY CARE) -> care_plan_type_community_care\n  CARE PLAN CREATION DATE -> care_plan_creation_date\n  CARE PLAN CREATION TIME -> care_plan_creation_time\n  CARE PLAN LAST UPDATED DATE -> care_plan_last_updated_date\n  CARE PLAN LAST UPDATED TIME -> care_plan_last_updated_time\n  CARE PLAN IMPLEMENTATION DATE -> care_plan_implementation_date\n  EFFECTIVE FROM -> effective_from\n  RECORD NUMBER -> record_number\n  CYP004 UNIQUE ID -> cyp004_unique_id\n  ORGANISATION IDENTIFIER (CODE OF PROVIDER) -> organisation_identifier_code_of_provider\n  PERSON ID -> person_id\n  UNIQUE SUBMISSION ID -> unique_submission_id\n  UNIQUE CARE PLAN IDENTIFIER -> unique_care_plan_identifier\n  RECORD START DATE -> record_start_date\n  RECORD END DATE -> record_end_date\n  UNIQUE MONTH ID -> unique_month_id\n  dmicImportLogId -> dmic_import_log_id\n  dmicSystemId -> dmic_system_id\n  dmicCCGCode -> dmic_ccg_code\n  dmicCCG -> dmic_ccg\n  Unique_LocalPatientId -> unique_local_patient_id\n  UniqueCYPHS_ID_Patient -> unique_cyphs_id_patient\n  FILE TYPE -> file_type\n  REPORTING PERIOD START DATE -> reporting_period_start_date\n  REPORTING PERIOD END DATE -> reporting_period_end_date"
    )
}}
select
    "SK" as sk,
    "CARE PLAN IDENTIFIER" as care_plan_identifier,
    "LOCAL PATIENT IDENTIFIER (EXTENDED)" as local_patient_identifier_extended,
    "CARE PLAN TYPE (COMMUNITY CARE)" as care_plan_type_community_care,
    "CARE PLAN CREATION DATE" as care_plan_creation_date,
    "CARE PLAN CREATION TIME" as care_plan_creation_time,
    "CARE PLAN LAST UPDATED DATE" as care_plan_last_updated_date,
    "CARE PLAN LAST UPDATED TIME" as care_plan_last_updated_time,
    "CARE PLAN IMPLEMENTATION DATE" as care_plan_implementation_date,
    "EFFECTIVE FROM" as effective_from,
    "RECORD NUMBER" as record_number,
    "CYP004 UNIQUE ID" as cyp004_unique_id,
    "ORGANISATION IDENTIFIER (CODE OF PROVIDER)" as organisation_identifier_code_of_provider,
    "PERSON ID" as person_id,
    "UNIQUE SUBMISSION ID" as unique_submission_id,
    "UNIQUE CARE PLAN IDENTIFIER" as unique_care_plan_identifier,
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
from {{ source('csds', 'CYP004CarePlanType') }}
