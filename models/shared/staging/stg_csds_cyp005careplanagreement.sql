-- Staging model for csds.CYP005CarePlanAgreement
-- Source: "DATA_LAKE"."CSDS"
-- Description: Community services dataset

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
