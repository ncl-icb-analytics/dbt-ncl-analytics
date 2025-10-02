-- Raw layer model for csds.CYP403CPP
-- Source: "DATA_LAKE"."CSDS"
-- Description: Community services dataset
-- This is a 1:1 passthrough from source with standardized column names
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
