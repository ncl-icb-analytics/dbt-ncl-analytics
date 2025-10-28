-- Raw layer model for mhsds.MHS609PresComp
-- Source: "DATA_LAKE"."MHSDS"
-- Description: Mental Health Services Data Set (MHSDS)
-- This is a 1:1 passthrough from source with standardized column names
select
    "SK" as sk,
    "ServiceRequestID" as service_request_id,
    "FindSchemeInUse" as find_scheme_in_use,
    "PresComp" as pres_comp,
    "PresCompCodSig" as pres_comp_cod_sig,
    "PresCompDate" as pres_comp_date,
    "RecordNumber" as record_number,
    "MHS609UniqID" as mhs609_uniq_id,
    "OrgIDProv" as org_id_prov,
    "Person_ID" as person_id,
    "UniqSubmissionID" as uniq_submission_id,
    "UniqServReqID" as uniq_serv_req_id,
    "UniqMonthID" as uniq_month_id,
    "RecordStartDate" as record_start_date,
    "RecordEndDate" as record_end_date,
    "EFFECTIVE_FROM" as effective_from,
    "dmicImportLogId" as dmic_import_log_id,
    "dmicSystemId" as dmic_system_id,
    "dmicCCGCode" as dmic_ccg_code,
    "dmicDateAdded" as dmic_date_added,
    "FileType" as file_type,
    "ReportingPeriodStartDate" as reporting_period_start_date,
    "ReportingPeriodEndDate" as reporting_period_end_date,
    "dmicDataset" as dmic_dataset,
    "RowNumber" as row_number
from {{ source('mhsds', 'MHS609PresComp') }}
