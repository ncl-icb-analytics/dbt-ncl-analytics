{{
    config(
        description="Raw layer (Mental Health Services Data Set (MHSDS)). 1:1 passthrough with cleaned column names. \nSource: DATA_LAKE.MHSDS.MHS902ServiceTeamDetails \ndbt: source(''mhsds'', ''MHS902ServiceTeamDetails'') \nColumns:\n  CareProfTeamLocalID -> care_prof_team_local_id\n  OrgIDCareProfTeamLocalID -> org_id_care_prof_team_local_id\n  ServTeamTypeMH -> serv_team_type_mh\n  ServTeamIntAgeGroup -> serv_team_int_age_group\n  MHS902UniqID -> mhs902_uniq_id\n  OrgIDProv -> org_id_prov\n  UniqSubmissionID -> uniq_submission_id\n  UniqMonthID -> uniq_month_id\n  EFFECTIVE_FROM -> effective_from\n  ServiceTypeName -> service_type_name\n  UniqCareProfTeamLocalID -> uniq_care_prof_team_local_id\n  RowNumber -> row_number\n  dmicImportLogId -> dmic_import_log_id\n  dmicSystemId -> dmic_system_id\n  dmicCCGCode -> dmic_ccg_code\n  dmicDateAdded -> dmic_date_added\n  FileType -> file_type\n  ReportingPeriodStartDate -> reporting_period_start_date\n  ReportingPeriodEndDate -> reporting_period_end_date\n  dmicDataset -> dmic_dataset"
    )
}}
select
    "CareProfTeamLocalID" as care_prof_team_local_id,
    "OrgIDCareProfTeamLocalID" as org_id_care_prof_team_local_id,
    "ServTeamTypeMH" as serv_team_type_mh,
    "ServTeamIntAgeGroup" as serv_team_int_age_group,
    "MHS902UniqID" as mhs902_uniq_id,
    "OrgIDProv" as org_id_prov,
    "UniqSubmissionID" as uniq_submission_id,
    "UniqMonthID" as uniq_month_id,
    "EFFECTIVE_FROM" as effective_from,
    "ServiceTypeName" as service_type_name,
    "UniqCareProfTeamLocalID" as uniq_care_prof_team_local_id,
    "RowNumber" as row_number,
    "dmicImportLogId" as dmic_import_log_id,
    "dmicSystemId" as dmic_system_id,
    "dmicCCGCode" as dmic_ccg_code,
    "dmicDateAdded" as dmic_date_added,
    "FileType" as file_type,
    "ReportingPeriodStartDate" as reporting_period_start_date,
    "ReportingPeriodEndDate" as reporting_period_end_date,
    "dmicDataset" as dmic_dataset
from {{ source('mhsds', 'MHS902ServiceTeamDetails') }}
