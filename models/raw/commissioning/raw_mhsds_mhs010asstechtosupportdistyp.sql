{{
    config(
        description="Raw layer (Mental Health Services Data Set (MHSDS)). 1:1 passthrough with cleaned column names. \nSource: DATA_LAKE.MHSDS.MHS010AssTechToSupportDisTyp \ndbt: source(''mhsds'', ''MHS010AssTechToSupportDisTyp'') \nColumns:\n  SK -> sk\n  LocalPatientId -> local_patient_id\n  AssistiveTechnologyFinding -> assistive_technology_finding\n  AssistiveTechnologyPrescTimestamp -> assistive_technology_presc_timestamp\n  AssistiveTechnologyPrescDate -> assistive_technology_presc_date\n  RecordNumber -> record_number\n  MHS010UniqID -> mhs010_uniq_id\n  OrgIDProv -> org_id_prov\n  Person_ID -> person_id\n  UniqSubmissionID -> uniq_submission_id\n  UniqMonthID -> uniq_month_id\n  RecordStartDate -> record_start_date\n  RecordEndDate -> record_end_date\n  EFFECTIVE_FROM -> effective_from\n  RowNumber -> row_number\n  dmicImportLogId -> dmic_import_log_id\n  dmicSystemId -> dmic_system_id\n  dmicCCGCode -> dmic_ccg_code\n  dmicDateAdded -> dmic_date_added\n  Unique_LocalPatientId -> unique_local_patient_id\n  FileType -> file_type\n  ReportingPeriodStartDate -> reporting_period_start_date\n  ReportingPeriodEndDate -> reporting_period_end_date\n  dmicDataset -> dmic_dataset"
    )
}}
select
    "SK" as sk,
    "LocalPatientId" as local_patient_id,
    "AssistiveTechnologyFinding" as assistive_technology_finding,
    "AssistiveTechnologyPrescTimestamp" as assistive_technology_presc_timestamp,
    "AssistiveTechnologyPrescDate" as assistive_technology_presc_date,
    "RecordNumber" as record_number,
    "MHS010UniqID" as mhs010_uniq_id,
    "OrgIDProv" as org_id_prov,
    "Person_ID" as person_id,
    "UniqSubmissionID" as uniq_submission_id,
    "UniqMonthID" as uniq_month_id,
    "RecordStartDate" as record_start_date,
    "RecordEndDate" as record_end_date,
    "EFFECTIVE_FROM" as effective_from,
    "RowNumber" as row_number,
    "dmicImportLogId" as dmic_import_log_id,
    "dmicSystemId" as dmic_system_id,
    "dmicCCGCode" as dmic_ccg_code,
    "dmicDateAdded" as dmic_date_added,
    "Unique_LocalPatientId" as unique_local_patient_id,
    "FileType" as file_type,
    "ReportingPeriodStartDate" as reporting_period_start_date,
    "ReportingPeriodEndDate" as reporting_period_end_date,
    "dmicDataset" as dmic_dataset
from {{ source('mhsds', 'MHS010AssTechToSupportDisTyp') }}
