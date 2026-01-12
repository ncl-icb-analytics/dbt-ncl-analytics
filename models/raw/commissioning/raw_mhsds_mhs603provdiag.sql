{{
    config(
        description="Raw layer (Mental Health Services Data Set (MHSDS)). 1:1 passthrough with cleaned column names. \nSource: DATA_LAKE.MHSDS.MHS603ProvDiag \ndbt: source(''mhsds'', ''MHS603ProvDiag'') \nColumns:\n  SK -> sk\n  ServiceRequestId -> service_request_id\n  DiagSchemeInUse -> diag_scheme_in_use\n  ProvDiag -> prov_diag\n  CodedProvDiagTimestamp -> coded_prov_diag_timestamp\n  ProvDiagDate -> prov_diag_date\n  RecordNumber -> record_number\n  MHS603UniqID -> mhs603_uniq_id\n  OrgIDProv -> org_id_prov\n  Person_ID -> person_id\n  UniqSubmissionID -> uniq_submission_id\n  UniqServReqID -> uniq_serv_req_id\n  UniqMonthID -> uniq_month_id\n  RecordStartDate -> record_start_date\n  RecordEndDate -> record_end_date\n  EFFECTIVE_FROM -> effective_from\n  MasterSnomedCTProvDiagCode -> master_snomed_ct_prov_diag_code\n  RowNumber -> row_number\n  dmicImportLogId -> dmic_import_log_id\n  dmicSystemId -> dmic_system_id\n  dmicCCGCode -> dmic_ccg_code\n  dmicDateAdded -> dmic_date_added\n  FileType -> file_type\n  ReportingPeriodStartDate -> reporting_period_start_date\n  ReportingPeriodEndDate -> reporting_period_end_date\n  dmicDataset -> dmic_dataset"
    )
}}
select
    "SK" as sk,
    "ServiceRequestId" as service_request_id,
    "DiagSchemeInUse" as diag_scheme_in_use,
    "ProvDiag" as prov_diag,
    "CodedProvDiagTimestamp" as coded_prov_diag_timestamp,
    "ProvDiagDate" as prov_diag_date,
    "RecordNumber" as record_number,
    "MHS603UniqID" as mhs603_uniq_id,
    "OrgIDProv" as org_id_prov,
    "Person_ID" as person_id,
    "UniqSubmissionID" as uniq_submission_id,
    "UniqServReqID" as uniq_serv_req_id,
    "UniqMonthID" as uniq_month_id,
    "RecordStartDate" as record_start_date,
    "RecordEndDate" as record_end_date,
    "EFFECTIVE_FROM" as effective_from,
    "MasterSnomedCTProvDiagCode" as master_snomed_ct_prov_diag_code,
    "RowNumber" as row_number,
    "dmicImportLogId" as dmic_import_log_id,
    "dmicSystemId" as dmic_system_id,
    "dmicCCGCode" as dmic_ccg_code,
    "dmicDateAdded" as dmic_date_added,
    "FileType" as file_type,
    "ReportingPeriodStartDate" as reporting_period_start_date,
    "ReportingPeriodEndDate" as reporting_period_end_date,
    "dmicDataset" as dmic_dataset
from {{ source('mhsds', 'MHS603ProvDiag') }}
