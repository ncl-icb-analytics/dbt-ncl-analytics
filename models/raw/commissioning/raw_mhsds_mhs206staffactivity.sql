-- Raw layer model for mhsds.MHS206StaffActivity
-- Source: "DATA_LAKE"."MHSDS"
-- Description: Mental Health Services Data Set (MHSDS)
-- This is a 1:1 passthrough from source with standardized column names
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
