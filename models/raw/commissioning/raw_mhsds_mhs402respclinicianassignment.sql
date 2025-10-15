-- Raw layer model for mhsds.MHS402RespClinicianAssignment
-- Source: "DATA_LAKE"."MHSDS"
-- Description: Mental Health Services Data Set (MHSDS)
-- This is a 1:1 passthrough from source with standardized column names
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
