{{
    config(
        description="Raw layer (Mental Health Services Data Set (MHSDS)). 1:1 passthrough with cleaned column names. \nSource: DATA_LAKE.MHSDS.MHS203OtherAttend \ndbt: source(''mhsds'', ''MHS203OtherAttend'') \nColumns:\n  SK -> sk\n  CareContactId -> care_contact_id\n  OtherPersonInAttend -> other_person_in_attend\n  ReasonPatientNoIMCA -> reason_patient_no_imca\n  ReasonPatientNoIMHA -> reason_patient_no_imha\n  RecordNumber -> record_number\n  MHS203UniqID -> mhs203_uniq_id\n  OrgIDProv -> org_id_prov\n  Person_ID -> person_id\n  UniqSubmissionID -> uniq_submission_id\n  UniqServReqID -> uniq_serv_req_id\n  UniqCareContID -> uniq_care_cont_id\n  UniqMonthID -> uniq_month_id\n  EFFECTIVE_FROM -> effective_from\n  RowNumber -> row_number\n  dmicImportLogId -> dmic_import_log_id\n  dmicSystemId -> dmic_system_id\n  dmicCCGCode -> dmic_ccg_code\n  dmicDateAdded -> dmic_date_added\n  FileType -> file_type\n  ReportingPeriodStartDate -> reporting_period_start_date\n  ReportingPeriodEndDate -> reporting_period_end_date\n  dmicDataset -> dmic_dataset"
    )
}}
select
    "SK" as sk,
    "CareContactId" as care_contact_id,
    "OtherPersonInAttend" as other_person_in_attend,
    "ReasonPatientNoIMCA" as reason_patient_no_imca,
    "ReasonPatientNoIMHA" as reason_patient_no_imha,
    "RecordNumber" as record_number,
    "MHS203UniqID" as mhs203_uniq_id,
    "OrgIDProv" as org_id_prov,
    "Person_ID" as person_id,
    "UniqSubmissionID" as uniq_submission_id,
    "UniqServReqID" as uniq_serv_req_id,
    "UniqCareContID" as uniq_care_cont_id,
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
from {{ source('mhsds', 'MHS203OtherAttend') }}
