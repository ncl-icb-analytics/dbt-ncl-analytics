{{
    config(
        description="Raw layer (Mental Health Services Data Set (MHSDS)). 1:1 passthrough with cleaned column names. \nSource: DATA_LAKE.MHSDS.MHS206StaffActivity \ndbt: source(''mhsds'', ''MHS206StaffActivity'') \nColumns:\n  CareActID -> care_act_id\n  CareProfLocalID -> care_prof_local_id\n  RecordNumber -> record_number\n  MHS206UniqID -> mhs206_uniq_id\n  OrgIDProv -> org_id_prov\n  UniqSubmissionID -> uniq_submission_id\n  UniqCareContID -> uniq_care_cont_id\n  UniqMonthID -> uniq_month_id\n  EFFECTIVE_FROM -> effective_from\n  dmicImportLogId -> dmic_import_log_id\n  dmicSystemId -> dmic_system_id\n  dmicCCGCode -> dmic_ccg_code\n  dmicDateAdded -> dmic_date_added\n  FileType -> file_type\n  ReportingPeriodStartDate -> reporting_period_start_date\n  ReportingPeriodEndDate -> reporting_period_end_date\n  dmicDataset -> dmic_dataset\n  SK -> sk\n  UniqCareProfLocalID -> uniq_care_prof_local_id\n  UniqCareActID -> uniq_care_act_id\n  Person_ID -> person_id\n  UniqServReqId -> uniq_serv_req_id\n  RowNumber -> row_number"
    )
}}
select
    "CareActID" as care_act_id,
    "CareProfLocalID" as care_prof_local_id,
    "RecordNumber" as record_number,
    "MHS206UniqID" as mhs206_uniq_id,
    "OrgIDProv" as org_id_prov,
    "UniqSubmissionID" as uniq_submission_id,
    "UniqCareContID" as uniq_care_cont_id,
    "UniqMonthID" as uniq_month_id,
    "EFFECTIVE_FROM" as effective_from,
    "dmicImportLogId" as dmic_import_log_id,
    "dmicSystemId" as dmic_system_id,
    "dmicCCGCode" as dmic_ccg_code,
    "dmicDateAdded" as dmic_date_added,
    "FileType" as file_type,
    "ReportingPeriodStartDate" as reporting_period_start_date,
    "ReportingPeriodEndDate" as reporting_period_end_date,
    "dmicDataset" as dmic_dataset,
    "SK" as sk,
    "UniqCareProfLocalID" as uniq_care_prof_local_id,
    "UniqCareActID" as uniq_care_act_id,
    "Person_ID" as person_id,
    "UniqServReqId" as uniq_serv_req_id,
    "RowNumber" as row_number
from {{ source('mhsds', 'MHS206StaffActivity') }}
