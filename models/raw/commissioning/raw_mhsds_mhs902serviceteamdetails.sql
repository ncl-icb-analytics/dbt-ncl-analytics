-- Raw layer model for mhsds.MHS902ServiceTeamDetails
-- Source: "DATA_LAKE"."MHSDS"
-- Description: Mental Health Services Data Set (MHSDS)
-- This is a 1:1 passthrough from source with standardized column names
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
