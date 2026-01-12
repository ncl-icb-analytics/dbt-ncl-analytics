{{
    config(
        description="Raw layer (Community services dataset). 1:1 passthrough with cleaned column names. \nSource: DATA_LAKE.CSDS.CYP005CarePlanAgreement \ndbt: source(''csds'', ''CYP005CarePlanAgreement'') \nColumns:\n  SK -> sk\n  CARE PLAN IDENTIFIER -> care_plan_identifier\n  CARE PLAN CONTENT AGREED BY -> care_plan_content_agreed_by\n  CARE PLAN AGREED BY -> care_plan_agreed_by\n  CARE PLAN CONTENT AGREED DATE -> care_plan_content_agreed_date\n  CARE PLAN AGREED DATE -> care_plan_agreed_date\n  CARE PLAN CONTENT AGREED TIME -> care_plan_content_agreed_time\n  CARE PLAN AGREED TIME -> care_plan_agreed_time\n  EFFECTIVE FROM -> effective_from\n  RECORD NUMBER -> record_number\n  CYP005 UNIQUE ID -> cyp005_unique_id\n  ORGANISATION IDENTIFIER (CODE OF PROVIDER) -> organisation_identifier_code_of_provider\n  PERSON ID -> person_id\n  UNIQUE SUBMISSION ID -> unique_submission_id\n  UNIQUE CARE PLAN IDENTIFIER -> unique_care_plan_identifier\n  RECORD START DATE -> record_start_date\n  RECORD END DATE -> record_end_date\n  UNIQUE MONTH ID -> unique_month_id\n  dmicImportLogId -> dmic_import_log_id\n  dmicSystemId -> dmic_system_id\n  dmicCCGCode -> dmic_ccg_code\n  dmicCCG -> dmic_ccg\n  UniqueCYPHS_ID_Patient -> unique_cyphs_id_patient\n  FILE TYPE -> file_type\n  REPORTING PERIOD START DATE -> reporting_period_start_date\n  REPORTING PERIOD END DATE -> reporting_period_end_date"
    )
}}
select
    "SK" as sk,
    "CARE PLAN IDENTIFIER" as care_plan_identifier,
    "CARE PLAN CONTENT AGREED BY" as care_plan_content_agreed_by,
    "CARE PLAN AGREED BY" as care_plan_agreed_by,
    "CARE PLAN CONTENT AGREED DATE" as care_plan_content_agreed_date,
    "CARE PLAN AGREED DATE" as care_plan_agreed_date,
    "CARE PLAN CONTENT AGREED TIME" as care_plan_content_agreed_time,
    "CARE PLAN AGREED TIME" as care_plan_agreed_time,
    "EFFECTIVE FROM" as effective_from,
    "RECORD NUMBER" as record_number,
    "CYP005 UNIQUE ID" as cyp005_unique_id,
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
    "UniqueCYPHS_ID_Patient" as unique_cyphs_id_patient,
    "FILE TYPE" as file_type,
    "REPORTING PERIOD START DATE" as reporting_period_start_date,
    "REPORTING PERIOD END DATE" as reporting_period_end_date
from {{ source('csds', 'CYP005CarePlanAgreement') }}
