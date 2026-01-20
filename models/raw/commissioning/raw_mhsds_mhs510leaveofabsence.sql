{{
    config(
        description="Raw layer (Mental Health Services Data Set (MHSDS)). 1:1 passthrough with cleaned column names. \nSource: DATA_LAKE.MHSDS.MHS510LeaveOfAbsence \ndbt: source(''mhsds'', ''MHS510LeaveOfAbsence'') \nColumns:\n  SK -> sk\n  WardStayId -> ward_stay_id\n  StartDateMHLeaveAbs -> start_date_mh_leave_abs\n  StartTimeMHLeaveAbs -> start_time_mh_leave_abs\n  EndDateMHLeaveAbs -> end_date_mh_leave_abs\n  EndTimeMHLeaveAbs -> end_time_mh_leave_abs\n  MHLeaveAbsEndReason -> mh_leave_abs_end_reason\n  EscortedLeaveIndicator -> escorted_leave_indicator\n  RecordNumber -> record_number\n  MHS510UniqID -> mhs510_uniq_id\n  OrgIDProv -> org_id_prov\n  Person_ID -> person_id\n  UniqSubmissionID -> uniq_submission_id\n  UniqServReqID -> uniq_serv_req_id\n  UniqWardStayID -> uniq_ward_stay_id\n  UniqHospProvSpellID -> uniq_hosp_prov_spell_id\n  UniqHospProvSpellNum -> uniq_hosp_prov_spell_num\n  UniqMonthID -> uniq_month_id\n  RecordStartDate -> record_start_date\n  RecordEndDate -> record_end_date\n  LOADaysRP -> loa_days_rp\n  EFFECTIVE_FROM -> effective_from\n  UniqWardCode -> uniq_ward_code\n  RowNumber -> row_number\n  dmicImportLogId -> dmic_import_log_id\n  dmicSystemId -> dmic_system_id\n  dmicCCGCode -> dmic_ccg_code\n  dmicDateAdded -> dmic_date_added\n  FileType -> file_type\n  ReportingPeriodStartDate -> reporting_period_start_date\n  ReportingPeriodEndDate -> reporting_period_end_date\n  dmicDataset -> dmic_dataset"
    )
}}
select
    "SK" as sk,
    "WardStayId" as ward_stay_id,
    "StartDateMHLeaveAbs" as start_date_mh_leave_abs,
    "StartTimeMHLeaveAbs" as start_time_mh_leave_abs,
    "EndDateMHLeaveAbs" as end_date_mh_leave_abs,
    "EndTimeMHLeaveAbs" as end_time_mh_leave_abs,
    "MHLeaveAbsEndReason" as mh_leave_abs_end_reason,
    "EscortedLeaveIndicator" as escorted_leave_indicator,
    "RecordNumber" as record_number,
    "MHS510UniqID" as mhs510_uniq_id,
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
    "LOADaysRP" as loa_days_rp,
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
from {{ source('mhsds', 'MHS510LeaveOfAbsence') }}
