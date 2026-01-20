{{
    config(
        description="Raw layer (Mental Health Services Data Set (MHSDS)). 1:1 passthrough with cleaned column names. \nSource: DATA_LAKE.MHSDS.MHS009CarePlanAgreement \ndbt: source(''mhsds'', ''MHS009CarePlanAgreement'') \nColumns:\n  SK -> sk\n  CarePlanID -> care_plan_id\n  FamilyCarePlanIndicator -> family_care_plan_indicator\n  NoFamilyCarePlanReason -> no_family_care_plan_reason\n  CarePlanContentAgreedBy -> care_plan_content_agreed_by\n  CarePlanAgreedBy -> care_plan_agreed_by\n  CarePlanContentAgreedDate -> care_plan_content_agreed_date\n  CarePlanAgreedDate -> care_plan_agreed_date\n  CarePlanContentAgreedTime -> care_plan_content_agreed_time\n  CarePlanAgreedTime -> care_plan_agreed_time\n  RecordNumber -> record_number\n  MHS009UniqID -> mhs009_uniq_id\n  OrgIDProv -> org_id_prov\n  Person_ID -> person_id\n  UniqSubmissionID -> uniq_submission_id\n  UniqCarePlanID -> uniq_care_plan_id\n  UniqMonthID -> uniq_month_id\n  RecordStartDate -> record_start_date\n  RecordEndDate -> record_end_date\n  EFFECTIVE_FROM -> effective_from\n  RowNumber -> row_number\n  dmicImportLogId -> dmic_import_log_id\n  dmicSystemId -> dmic_system_id\n  dmicCCGCode -> dmic_ccg_code\n  dmicDateAdded -> dmic_date_added\n  FileType -> file_type\n  ReportingPeriodStartDate -> reporting_period_start_date\n  ReportingPeriodEndDate -> reporting_period_end_date\n  dmicDataset -> dmic_dataset"
    )
}}
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
