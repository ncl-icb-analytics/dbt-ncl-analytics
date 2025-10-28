-- Raw layer model for mhsds.MHS901StaffDetails
-- Source: "DATA_LAKE"."MHSDS"
-- Description: Mental Health Services Data Set (MHSDS)
-- This is a 1:1 passthrough from source with standardized column names
select
    "CareProfLocalId" as care_prof_local_id,
    "OrgIDCareProfLocalID" as org_id_care_prof_local_id,
    "ProfRegBodyCode" as prof_reg_body_code,
    "ProfRegEntryId" as prof_reg_entry_id,
    "CareProfStaffGpMH" as care_prof_staff_gp_mh,
    "MainSpecCodeMH" as main_spec_code_mh,
    "OccCode" as occ_code,
    "CareProfJobRoleCode" as care_prof_job_role_code,
    "MHS901UniqID" as mhs901_uniq_id,
    "OrgIDProv" as org_id_prov,
    "UniqSubmissionID" as uniq_submission_id,
    "UniqCareProfLocalID" as uniq_care_prof_local_id,
    "UniqMonthID" as uniq_month_id,
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
from {{ source('mhsds', 'MHS901StaffDetails') }}
