-- Raw layer model for mhsds.MHS009CarePlanAgreement
-- Source: "DATA_LAKE"."MHSDS"
-- Description: Mental Health Services Data Set (MHSDS)
-- This is a 1:1 passthrough from source with standardized column names
select
    "SK" as sk,
    "CarePlanID" as care_plan_id,
    "FamilyCarePlanIndicator" as family_care_plan_indicator,
    "NoFamilyCarePlanReason" as no_family_care_plan_reason,
    "CarePlanContentAgreedBy" as care_plan_content_agreed_by,
    "CarePlanAgreedBy" as care_plan_agreed_by,
    "CarePlanContentAgreedDate" as care_plan_content_agreed_date,
    "CarePlanAgreedDate" as care_plan_agreed_date,
    "CarePlanContentAgreedTime" as care_plan_content_agreed_time,
    "CarePlanAgreedTime" as care_plan_agreed_time,
    "RecordNumber" as record_number,
    "MHS009UniqID" as mhs009_uniq_id,
    "OrgIDProv" as org_id_prov,
    "Person_ID" as person_id,
    "UniqSubmissionID" as uniq_submission_id,
    "UniqCarePlanID" as uniq_care_plan_id,
    "UniqMonthID" as uniq_month_id,
    "RecordStartDate" as record_start_date,
    "RecordEndDate" as record_end_date,
    "EFFECTIVE_FROM" as effective_from,
    "RowNumber" as row_number,
    "dmicImportLogId" as dmic_import_log_id,
    "dmicSystemId" as dmic_system_id,
    "dmicCCGCode" as dmic_ccg_code,
    "dmicDateAdded" as dmic_date_added,
    "FileType" as file_type,
    "ReportingPeriodStartDate" as reporting_period_start_date,
    "ReportingPeriodEndDate" as reporting_period_end_date,
    "dmicDataset" as dmic_dataset
from {{ source('mhsds', 'MHS009CarePlanAgreement') }}
