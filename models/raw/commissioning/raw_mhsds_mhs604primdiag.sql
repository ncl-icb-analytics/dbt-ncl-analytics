{{
    config(
        description="Raw layer (Mental Health Services Data Set (MHSDS)). 1:1 passthrough with cleaned column names. \nSource: DATA_LAKE.MHSDS.MHS604PrimDiag \ndbt: source(''mhsds'', ''MHS604PrimDiag'') \nColumns:\n  SK -> sk\n  ServiceRequestId -> service_request_id\n  DiagSchemeInUse -> diag_scheme_in_use\n  PrimDiag -> prim_diag\n  CodedDiagTimestamp -> coded_diag_timestamp\n  DiagDate -> diag_date\n  RecordNumber -> record_number\n  MHS604UniqID -> mhs604_uniq_id\n  OrgIDProv -> org_id_prov\n  Person_ID -> person_id\n  UniqSubmissionID -> uniq_submission_id\n  UniqServReqID -> uniq_serv_req_id\n  UniqMonthID -> uniq_month_id\n  RecordStartDate -> record_start_date\n  RecordEndDate -> record_end_date\n  EFFECTIVE_FROM -> effective_from\n  MasterSnomedCTPrimDiagCode -> master_snomed_ct_prim_diag_code\n  RowNumber -> row_number\n  dmicImportLogId -> dmic_import_log_id\n  dmicSystemId -> dmic_system_id\n  dmicCCGCode -> dmic_ccg_code\n  dmicDateAdded -> dmic_date_added\n  FileType -> file_type\n  ReportingPeriodStartDate -> reporting_period_start_date\n  ReportingPeriodEndDate -> reporting_period_end_date\n  dmicDataset -> dmic_dataset"
    )
}}
select
    "SK" as sk,
    "ServiceRequestId" as service_request_id,
    "DiagSchemeInUse" as diag_scheme_in_use,
    "PrimDiag" as prim_diag,
    "CodedDiagTimestamp" as coded_diag_timestamp,
    "DiagDate" as diag_date,
    "RecordNumber" as record_number,
    "MHS604UniqID" as mhs604_uniq_id,
    "OrgIDProv" as org_id_prov,
    "Person_ID" as person_id,
    "UniqSubmissionID" as uniq_submission_id,
    "UniqServReqID" as uniq_serv_req_id,
    "UniqMonthID" as uniq_month_id,
    "RecordStartDate" as record_start_date,
    "RecordEndDate" as record_end_date,
    "EFFECTIVE_FROM" as effective_from,
    "MasterSnomedCTPrimDiagCode" as master_snomed_ct_prim_diag_code,
    "RowNumber" as row_number,
    "dmicImportLogId" as dmic_import_log_id,
    "dmicSystemId" as dmic_system_id,
    "dmicCCGCode" as dmic_ccg_code,
    "dmicDateAdded" as dmic_date_added,
    "FileType" as file_type,
    "ReportingPeriodStartDate" as reporting_period_start_date,
    "ReportingPeriodEndDate" as reporting_period_end_date,
    "dmicDataset" as dmic_dataset
from {{ source('mhsds', 'MHS604PrimDiag') }}
