-- Raw layer model for mhsds.MHS105OnwardReferral
-- Source: "DATA_LAKE"."MHSDS"
-- Description: Mental Health Services Data Set (MHSDS)
-- This is a 1:1 passthrough from source with standardized column names
select
    "SK" as sk,
    "ServiceRequestId" as service_request_id,
    "DecisionToReferDate" as decision_to_refer_date,
    "DecisionToReferTime" as decision_to_refer_time,
    "OnwardReferDate" as onward_refer_date,
    "OnwardReferTime" as onward_refer_time,
    "OnwardReferReason" as onward_refer_reason,
    "OATReason" as oat_reason,
    "OrgIDReceiving" as org_id_receiving,
    "ReferralProc" as referral_proc,
    "CodeRefProcAndProcStatus" as code_ref_proc_and_proc_status,
    "RecordNumber" as record_number,
    "MHS105UniqID" as mhs105_uniq_id,
    "OrgIDProv" as org_id_prov,
    "Person_ID" as person_id,
    "UniqSubmissionID" as uniq_submission_id,
    "UniqServReqID" as uniq_serv_req_id,
    "UniqMonthID" as uniq_month_id,
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
from {{ source('mhsds', 'MHS105OnwardReferral') }}
