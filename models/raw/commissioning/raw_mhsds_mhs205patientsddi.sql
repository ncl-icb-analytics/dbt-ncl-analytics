{{
    config(
        description="Raw layer (Mental Health Services Data Set (MHSDS)). 1:1 passthrough with cleaned column names. \nSource: DATA_LAKE.MHSDS.MHS205PatientSDDI \ndbt: source(''mhsds'', ''MHS205PatientSDDI'') \nColumns:\n  SK -> sk\n  ServiceRequestID -> service_request_id\n  OrgIDPSDDIProvider -> org_idpsddi_provider\n  StartDatePSDDI -> start_date_psddi\n  EndDatePSDDI -> end_date_psddi\n  PSDDIMech -> psddi_mech\n  PSDDIProcedure -> psddi_procedure\n  RecordNumber -> record_number\n  MHS205UniqID -> mhs205_uniq_id\n  OrgIDProv -> org_id_prov\n  Person_ID -> person_id\n  UniqSubmissionID -> uniq_submission_id\n  UniqServReqID -> uniq_serv_req_id\n  UniqMonthID -> uniq_month_id\n  EFFECTIVE_FROM -> effective_from\n  dmicImportLogId -> dmic_import_log_id\n  dmicSystemId -> dmic_system_id\n  dmicCCGCode -> dmic_ccg_code\n  dmicDateAdded -> dmic_date_added\n  FileType -> file_type\n  ReportingPeriodStartDate -> reporting_period_start_date\n  ReportingPeriodEndDate -> reporting_period_end_date\n  dmicDataset -> dmic_dataset\n  RowNumber -> row_number"
    )
}}
select
    "SK" as sk,
    "ServiceRequestID" as service_request_id,
    "OrgIDPSDDIProvider" as org_idpsddi_provider,
    "StartDatePSDDI" as start_date_psddi,
    "EndDatePSDDI" as end_date_psddi,
    "PSDDIMech" as psddi_mech,
    "PSDDIProcedure" as psddi_procedure,
    "RecordNumber" as record_number,
    "MHS205UniqID" as mhs205_uniq_id,
    "OrgIDProv" as org_id_prov,
    "Person_ID" as person_id,
    "UniqSubmissionID" as uniq_submission_id,
    "UniqServReqID" as uniq_serv_req_id,
    "UniqMonthID" as uniq_month_id,
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
from {{ source('mhsds', 'MHS205PatientSDDI') }}
