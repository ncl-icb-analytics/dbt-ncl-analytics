-- Raw layer model for mhsds.MHS514TrialLeave
-- Source: "DATA_LAKE"."MHSDS"
-- Description: Mental Health Services Data Set (MHSDS)
-- This is a 1:1 passthrough from source with standardized column names
select
    "SK" as sk,
    "WardStayId" as ward_stay_id,
    "StartDateMHTrialLeave" as start_date_mh_trial_leave,
    "StartTimeMHTrialLeave" as start_time_mh_trial_leave,
    "EndDateMHTrialLeave" as end_date_mh_trial_leave,
    "EndTimeMHTrialLeave" as end_time_mh_trial_leave,
    "RecordNumber" as record_number,
    "MHS514UniqID" as mhs514_uniq_id,
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
from {{ source('mhsds', 'MHS514TrialLeave') }}
