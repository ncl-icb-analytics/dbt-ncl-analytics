{{
    config(
        description="Raw layer (Mental Health Services Data Set (MHSDS)). 1:1 passthrough with cleaned column names. \nSource: DATA_LAKE.MHSDS.MHS516PoliceAssistanceRequest \ndbt: source(''mhsds'', ''MHS516PoliceAssistanceRequest'') \nColumns:\n  SK -> sk\n  WardStayId -> ward_stay_id\n  PoliceAssistReqDate -> police_assist_req_date\n  PoliceAssistReqTime -> police_assist_req_time\n  PoliceAssistArrDate -> police_assist_arr_date\n  PoliceAssistArrTime -> police_assist_arr_time\n  PoliceRestraintForceUsedInd -> police_restraint_force_used_ind\n  RecordNumber -> record_number\n  MHS516UniqID -> mhs516_uniq_id\n  OrgIDProv -> org_id_prov\n  Person_ID -> person_id\n  UniqSubmissionID -> uniq_submission_id\n  UniqServReqID -> uniq_serv_req_id\n  UniqHospProvSpellID -> uniq_hosp_prov_spell_id\n  UniqWardStayID -> uniq_ward_stay_id\n  UniqMonthID -> uniq_month_id\n  EFFECTIVE_FROM -> effective_from\n  UniqWardCode -> uniq_ward_code\n  RowNumber -> row_number\n  dmicImportLogId -> dmic_import_log_id\n  dmicSystemId -> dmic_system_id\n  dmicCCGCode -> dmic_ccg_code\n  dmicDateAdded -> dmic_date_added\n  FileType -> file_type\n  ReportingPeriodStartDate -> reporting_period_start_date\n  ReportingPeriodEndDate -> reporting_period_end_date\n  dmicDataset -> dmic_dataset"
    )
}}
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
