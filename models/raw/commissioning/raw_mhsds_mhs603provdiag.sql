-- Raw layer model for mhsds.MHS603ProvDiag
-- Source: "DATA_LAKE"."MHSDS"
-- Description: Mental Health Services Data Set (MHSDS)
-- This is a 1:1 passthrough from source with standardized column names
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
