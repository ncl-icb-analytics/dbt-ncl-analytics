{{
    config(
        description="Raw layer (Mental Health Services Data Set (MHSDS)). 1:1 passthrough with cleaned column names. \nSource: DATA_LAKE.MHSDS.MHS901StaffDetails \ndbt: source(''mhsds'', ''MHS901StaffDetails'') \nColumns:\n  CareProfLocalId -> care_prof_local_id\n  OrgIDCareProfLocalID -> org_id_care_prof_local_id\n  ProfRegBodyCode -> prof_reg_body_code\n  ProfRegEntryId -> prof_reg_entry_id\n  CareProfStaffGpMH -> care_prof_staff_gp_mh\n  MainSpecCodeMH -> main_spec_code_mh\n  OccCode -> occ_code\n  CareProfJobRoleCode -> care_prof_job_role_code\n  MHS901UniqID -> mhs901_uniq_id\n  OrgIDProv -> org_id_prov\n  UniqSubmissionID -> uniq_submission_id\n  UniqCareProfLocalID -> uniq_care_prof_local_id\n  UniqMonthID -> uniq_month_id\n  EFFECTIVE_FROM -> effective_from\n  RowNumber -> row_number\n  dmicImportLogId -> dmic_import_log_id\n  dmicSystemId -> dmic_system_id\n  dmicCCGCode -> dmic_ccg_code\n  dmicDateAdded -> dmic_date_added\n  FileType -> file_type\n  ReportingPeriodStartDate -> reporting_period_start_date\n  ReportingPeriodEndDate -> reporting_period_end_date\n  dmicDataset -> dmic_dataset"
    )
}}
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
