{{
    config(
        description="Raw layer (Mental Health Services Data Set (MHSDS)). 1:1 passthrough with cleaned column names. \nSource: DATA_LAKE.MHSDS.MHS509HomeLeave \ndbt: source(''mhsds'', ''MHS509HomeLeave'') \nColumns:\n  SK -> sk\n  WardStayId -> ward_stay_id\n  StartDateHomeLeave -> start_date_home_leave\n  StartTimeHomeLeave -> start_time_home_leave\n  EndDateHomeLeave -> end_date_home_leave\n  EndTimeHomeLeave -> end_time_home_leave\n  RecordNumber -> record_number\n  MHS509UniqID -> mhs509_uniq_id\n  OrgIDProv -> org_id_prov\n  Person_ID -> person_id\n  UniqSubmissionID -> uniq_submission_id\n  UniqServReqID -> uniq_serv_req_id\n  UniqWardStayID -> uniq_ward_stay_id\n  UniqHospProvSpellID -> uniq_hosp_prov_spell_id\n  UniqHospProvSpellNum -> uniq_hosp_prov_spell_num\n  UniqMonthID -> uniq_month_id\n  RecordStartDate -> record_start_date\n  RecordEndDate -> record_end_date\n  HomeLeaveDaysEndRP -> home_leave_days_end_rp\n  EFFECTIVE_FROM -> effective_from\n  UniqWardCode -> uniq_ward_code\n  RowNumber -> row_number\n  dmicImportLogId -> dmic_import_log_id\n  dmicSystemId -> dmic_system_id\n  dmicCCGCode -> dmic_ccg_code\n  dmicDateAdded -> dmic_date_added\n  FileType -> file_type\n  ReportingPeriodStartDate -> reporting_period_start_date\n  ReportingPeriodEndDate -> reporting_period_end_date\n  dmicDataset -> dmic_dataset"
    )
}}
select
    "SK" as sk,
    "WardStayId" as ward_stay_id,
    "StartDateHomeLeave" as start_date_home_leave,
    "StartTimeHomeLeave" as start_time_home_leave,
    "EndDateHomeLeave" as end_date_home_leave,
    "EndTimeHomeLeave" as end_time_home_leave,
    "RecordNumber" as record_number,
    "MHS509UniqID" as mhs509_uniq_id,
    "OrgIDProv" as org_id_prov,
    "Person_ID" as person_id,
    "UniqSubmissionID" as uniq_submission_id,
    "UniqServReqID" as uniq_serv_req_id,
    "UniqWardStayID" as uniq_ward_stay_id,
    "UniqHospProvSpellID" as uniq_hosp_prov_spell_id,
    "UniqHospProvSpellNum" as uniq_hosp_prov_spell_num,
    "UniqMonthID" as uniq_month_id,
    "RecordStartDate" as record_start_date,
    "RecordEndDate" as record_end_date,
    "HomeLeaveDaysEndRP" as home_leave_days_end_rp,
    "EFFECTIVE_FROM" as effective_from,
    "UniqWardCode" as uniq_ward_code,
    "RowNumber" as row_number,
    "dmicImportLogId" as dmic_import_log_id,
    "dmicSystemId" as dmic_system_id,
    "dmicCCGCode" as dmic_ccg_code,
    "dmicDateAdded" as dmic_date_added,
    "FileType" as file_type,
    "ReportingPeriodStartDate" as reporting_period_start_date,
    "ReportingPeriodEndDate" as reporting_period_end_date,
    "dmicDataset" as dmic_dataset
from {{ source('mhsds', 'MHS509HomeLeave') }}
