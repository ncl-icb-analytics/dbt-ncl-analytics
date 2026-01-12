{{
    config(
        description="Raw layer (Mental Health Services Data Set (MHSDS)). 1:1 passthrough with cleaned column names. \nSource: DATA_LAKE.MHSDS.MHS402RespClinicianAssignment \ndbt: source(''mhsds'', ''MHS402RespClinicianAssignment'') \nColumns:\n  SK -> sk\n  MHActLegalStatusClassPeriodId -> mh_act_legal_status_class_period_id\n  StartDateRespClinAssign -> start_date_resp_clin_assign\n  CareProfLocalId -> care_prof_local_id\n  EndDateRespClinAssign -> end_date_resp_clin_assign\n  RecordNumber -> record_number\n  MHS402UniqID -> mhs402_uniq_id\n  OrgIDProv -> org_id_prov\n  Person_ID -> person_id\n  UniqSubmissionID -> uniq_submission_id\n  UniqMHActEpisodeID -> uniq_mh_act_episode_id\n  UniqCareProfLocalID -> uniq_care_prof_local_id\n  UniqMonthID -> uniq_month_id\n  RecordStartDate -> record_start_date\n  RecordEndDate -> record_end_date\n  EFFECTIVE_FROM -> effective_from\n  RowNumber -> row_number\n  dmicImportLogId -> dmic_import_log_id\n  dmicSystemId -> dmic_system_id\n  dmicCCGCode -> dmic_ccg_code\n  dmicDateAdded -> dmic_date_added\n  FileType -> file_type\n  ReportingPeriodStartDate -> reporting_period_start_date\n  ReportingPeriodEndDate -> reporting_period_end_date\n  dmicDataset -> dmic_dataset"
    )
}}
select
    "SK" as sk,
    "MHActLegalStatusClassPeriodId" as mh_act_legal_status_class_period_id,
    "StartDateRespClinAssign" as start_date_resp_clin_assign,
    "CareProfLocalId" as care_prof_local_id,
    "EndDateRespClinAssign" as end_date_resp_clin_assign,
    "RecordNumber" as record_number,
    "MHS402UniqID" as mhs402_uniq_id,
    "OrgIDProv" as org_id_prov,
    "Person_ID" as person_id,
    "UniqSubmissionID" as uniq_submission_id,
    "UniqMHActEpisodeID" as uniq_mh_act_episode_id,
    "UniqCareProfLocalID" as uniq_care_prof_local_id,
    "UniqMonthID" as uniq_month_id,
    "RecordStartDate" as record_start_date,
    "RecordEndDate" as record_end_date,
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
from {{ source('mhsds', 'MHS402RespClinicianAssignment') }}
