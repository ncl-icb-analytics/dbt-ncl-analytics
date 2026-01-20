{{
    config(
        description="Raw layer (Mental Health Services Data Set (MHSDS)). 1:1 passthrough with cleaned column names. \nSource: DATA_LAKE.MHSDS.MHS106DischargePlanAgreement \ndbt: source(''mhsds'', ''MHS106DischargePlanAgreement'') \nColumns:\n  SK -> sk\n  ServiceRequestId -> service_request_id\n  DischPlanContentAgreedBy -> disch_plan_content_agreed_by\n  DischPlanAgreedBy -> disch_plan_agreed_by\n  DischPlanContentAgreedDate -> disch_plan_content_agreed_date\n  DischPlanAgreedDate -> disch_plan_agreed_date\n  DischPlanContentAgreedTime -> disch_plan_content_agreed_time\n  DischPlanAgreedTime -> disch_plan_agreed_time\n  RecordNumber -> record_number\n  MHS106UniqID -> mhs106_uniq_id\n  OrgIDProv -> org_id_prov\n  Person_ID -> person_id\n  UniqSubmissionID -> uniq_submission_id\n  UniqServReqID -> uniq_serv_req_id\n  UniqMonthID -> uniq_month_id\n  RecordStartDate -> record_start_date\n  RecordEndDate -> record_end_date\n  EFFECTIVE_FROM -> effective_from\n  RowNumber -> row_number\n  dmicImportLogId -> dmic_import_log_id\n  dmicSystemId -> dmic_system_id\n  dmicCCGCode -> dmic_ccg_code\n  dmicDateAdded -> dmic_date_added\n  FileType -> file_type\n  ReportingPeriodStartDate -> reporting_period_start_date\n  ReportingPeriodEndDate -> reporting_period_end_date\n  dmicDataset -> dmic_dataset"
    )
}}
select
    "SK" as sk,
    "ServiceRequestId" as service_request_id,
    "DischPlanContentAgreedBy" as disch_plan_content_agreed_by,
    "DischPlanAgreedBy" as disch_plan_agreed_by,
    "DischPlanContentAgreedDate" as disch_plan_content_agreed_date,
    "DischPlanAgreedDate" as disch_plan_agreed_date,
    "DischPlanContentAgreedTime" as disch_plan_content_agreed_time,
    "DischPlanAgreedTime" as disch_plan_agreed_time,
    "RecordNumber" as record_number,
    "MHS106UniqID" as mhs106_uniq_id,
    "OrgIDProv" as org_id_prov,
    "Person_ID" as person_id,
    "UniqSubmissionID" as uniq_submission_id,
    "UniqServReqID" as uniq_serv_req_id,
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
from {{ source('mhsds', 'MHS106DischargePlanAgreement') }}
