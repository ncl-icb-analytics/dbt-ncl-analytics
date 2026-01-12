{{
    config(
        description="Raw layer (Mental Health Services Data Set (MHSDS)). 1:1 passthrough with cleaned column names. \nSource: DATA_LAKE.MHSDS.MHS105OnwardReferral \ndbt: source(''mhsds'', ''MHS105OnwardReferral'') \nColumns:\n  SK -> sk\n  ServiceRequestId -> service_request_id\n  DecisionToReferDate -> decision_to_refer_date\n  DecisionToReferTime -> decision_to_refer_time\n  OnwardReferDate -> onward_refer_date\n  OnwardReferTime -> onward_refer_time\n  OnwardReferReason -> onward_refer_reason\n  OATReason -> oat_reason\n  OrgIDReceiving -> org_id_receiving\n  ReferralProc -> referral_proc\n  CodeRefProcAndProcStatus -> code_ref_proc_and_proc_status\n  RecordNumber -> record_number\n  MHS105UniqID -> mhs105_uniq_id\n  OrgIDProv -> org_id_prov\n  Person_ID -> person_id\n  UniqSubmissionID -> uniq_submission_id\n  UniqServReqID -> uniq_serv_req_id\n  UniqMonthID -> uniq_month_id\n  EFFECTIVE_FROM -> effective_from\n  RowNumber -> row_number\n  dmicImportLogId -> dmic_import_log_id\n  dmicSystemId -> dmic_system_id\n  dmicCCGCode -> dmic_ccg_code\n  dmicDateAdded -> dmic_date_added\n  FileType -> file_type\n  ReportingPeriodStartDate -> reporting_period_start_date\n  ReportingPeriodEndDate -> reporting_period_end_date\n  dmicDataset -> dmic_dataset"
    )
}}
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
