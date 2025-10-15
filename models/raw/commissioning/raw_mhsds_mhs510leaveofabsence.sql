-- Raw layer model for mhsds.MHS510LeaveOfAbsence
-- Source: "DATA_LAKE"."MHSDS"
-- Description: Mental Health Services Data Set (MHSDS)
-- This is a 1:1 passthrough from source with standardized column names
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
