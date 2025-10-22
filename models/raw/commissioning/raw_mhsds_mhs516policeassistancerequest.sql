-- Raw layer model for mhsds.MHS516PoliceAssistanceRequest
-- Source: "DATA_LAKE"."MHSDS"
-- Description: Mental Health Services Data Set (MHSDS)
-- This is a 1:1 passthrough from source with standardized column names
select
    "SK" as sk,
    "WardStayId" as ward_stay_id,
    "PoliceAssistReqDate" as police_assist_req_date,
    "PoliceAssistReqTime" as police_assist_req_time,
    "PoliceAssistArrDate" as police_assist_arr_date,
    "PoliceAssistArrTime" as police_assist_arr_time,
    "PoliceRestraintForceUsedInd" as police_restraint_force_used_ind,
    "RecordNumber" as record_number,
    "MHS516UniqID" as mhs516_uniq_id,
    "OrgIDProv" as org_id_prov,
    "Person_ID" as person_id,
    "UniqSubmissionID" as uniq_submission_id,
    "UniqServReqID" as uniq_serv_req_id,
    "UniqHospProvSpellID" as uniq_hosp_prov_spell_id,
    "UniqWardStayID" as uniq_ward_stay_id,
    "UniqMonthID" as uniq_month_id,
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
from {{ source('mhsds', 'MHS516PoliceAssistanceRequest') }}
