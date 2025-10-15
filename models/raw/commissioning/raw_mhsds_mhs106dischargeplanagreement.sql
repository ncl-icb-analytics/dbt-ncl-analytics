-- Raw layer model for mhsds.MHS106DischargePlanAgreement
-- Source: "DATA_LAKE"."MHSDS"
-- Description: Mental Health Services Data Set (MHSDS)
-- This is a 1:1 passthrough from source with standardized column names
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
