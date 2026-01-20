{{
    config(
        description="Raw layer (Mental Health Services Data Set (MHSDS)). 1:1 passthrough with cleaned column names. \nSource: DATA_LAKE.MHSDS.MHS401MHActPeriod \ndbt: source(''mhsds'', ''MHS401MHActPeriod'') \nColumns:\n  SK -> sk\n  MHActLegalStatusClassPeriodId -> mh_act_legal_status_class_period_id\n  LocalPatientId -> local_patient_id\n  StartDateMHActLegalStatusClass -> start_date_mh_act_legal_status_class\n  StartTimeMHActLegalStatusClass -> start_time_mh_act_legal_status_class\n  LegalStatusClassPeriodStartReason -> legal_status_class_period_start_reason\n  ExpiryDateMHActLegalStatusClass -> expiry_date_mh_act_legal_status_class\n  ExpiryTimeMHActLegalStatusClass -> expiry_time_mh_act_legal_status_class\n  EndDateMHActLegalStatusClass -> end_date_mh_act_legal_status_class\n  EndTimeMHActLegalStatusClass -> end_time_mh_act_legal_status_class\n  LegalStatusClassPeriodEndReason -> legal_status_class_period_end_reason\n  LegalStatusCode -> legal_status_code\n  MentalCat -> mental_cat\n  RecordNumber -> record_number\n  MHS401UniqID -> mhs401_uniq_id\n  OrgIDProv -> org_id_prov\n  Person_ID -> person_id\n  UniqSubmissionID -> uniq_submission_id\n  UniqMHActEpisodeID -> uniq_mh_act_episode_id\n  UniqMonthID -> uniq_month_id\n  NHSDLegalStatus -> nhsd_legal_status\n  RecordStartDate -> record_start_date\n  RecordEndDate -> record_end_date\n  InactTimeMHAPeriod -> inact_time_mha_period\n  EFFECTIVE_FROM -> effective_from\n  RowNumber -> row_number\n  dmicImportLogId -> dmic_import_log_id\n  dmicSystemId -> dmic_system_id\n  dmicCCGCode -> dmic_ccg_code\n  dmicDateAdded -> dmic_date_added\n  Unique_LocalPatientId -> unique_local_patient_id\n  FileType -> file_type\n  ReportingPeriodStartDate -> reporting_period_start_date\n  ReportingPeriodEndDate -> reporting_period_end_date\n  dmicDataset -> dmic_dataset"
    )
}}
select
    "SK" as sk,
    "MHActLegalStatusClassPeriodId" as mh_act_legal_status_class_period_id,
    "LocalPatientId" as local_patient_id,
    "StartDateMHActLegalStatusClass" as start_date_mh_act_legal_status_class,
    "StartTimeMHActLegalStatusClass" as start_time_mh_act_legal_status_class,
    "LegalStatusClassPeriodStartReason" as legal_status_class_period_start_reason,
    "ExpiryDateMHActLegalStatusClass" as expiry_date_mh_act_legal_status_class,
    "ExpiryTimeMHActLegalStatusClass" as expiry_time_mh_act_legal_status_class,
    "EndDateMHActLegalStatusClass" as end_date_mh_act_legal_status_class,
    "EndTimeMHActLegalStatusClass" as end_time_mh_act_legal_status_class,
    "LegalStatusClassPeriodEndReason" as legal_status_class_period_end_reason,
    "LegalStatusCode" as legal_status_code,
    "MentalCat" as mental_cat,
    "RecordNumber" as record_number,
    "MHS401UniqID" as mhs401_uniq_id,
    "OrgIDProv" as org_id_prov,
    "Person_ID" as person_id,
    "UniqSubmissionID" as uniq_submission_id,
    "UniqMHActEpisodeID" as uniq_mh_act_episode_id,
    "UniqMonthID" as uniq_month_id,
    "NHSDLegalStatus" as nhsd_legal_status,
    "RecordStartDate" as record_start_date,
    "RecordEndDate" as record_end_date,
    "InactTimeMHAPeriod" as inact_time_mha_period,
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
from {{ source('mhsds', 'MHS401MHActPeriod') }}
