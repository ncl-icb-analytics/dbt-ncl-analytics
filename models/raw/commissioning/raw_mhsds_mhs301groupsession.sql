{{
    config(
        description="Raw layer (Mental Health Services Data Set (MHSDS)). 1:1 passthrough with cleaned column names. \nSource: DATA_LAKE.MHSDS.MHS301GroupSession \ndbt: source(''mhsds'', ''MHS301GroupSession'') \nColumns:\n  SK -> sk\n  GroupSessId -> group_sess_id\n  GroupSessDate -> group_sess_date\n  OrgIDComm -> org_id_comm\n  ClinContDurOfGroupSess -> clin_cont_dur_of_group_sess\n  GroupSessType -> group_sess_type\n  NumberOfGroupSessParticip -> number_of_group_sess_particip\n  ActLocTypeCode -> act_loc_type_code\n  SiteIDOfTreat -> site_id_of_treat\n  CareProfLocalId -> care_prof_local_id\n  ServTeamTypeRefToMH -> serv_team_type_ref_to_mh\n  NHSServAgreeLineID -> nhs_serv_agree_line_id\n  NHSServAgreeLineNum -> nhs_serv_agree_line_num\n  MHS301UniqID -> mhs301_uniq_id\n  OrgIDProv -> org_id_prov\n  UniqSubmissionID -> uniq_submission_id\n  UniqCareProfLocalID -> uniq_care_prof_local_id\n  UniqMonthID -> uniq_month_id\n  EFFECTIVE_FROM -> effective_from\n  RowNumber -> row_number\n  dmicImportLogId -> dmic_import_log_id\n  UniqGroupSessId -> uniq_group_sess_id\n  dmicSystemId -> dmic_system_id\n  dmicCCGCode -> dmic_ccg_code\n  dmicDateAdded -> dmic_date_added\n  dmIcbCommissioner -> dm_icb_commissioner\n  dmSubIcbCommissioner -> dm_sub_icb_commissioner\n  dmCommissionerDerivationReason -> dm_commissioner_derivation_reason\n  FileType -> file_type\n  ReportingPeriodStartDate -> reporting_period_start_date\n  ReportingPeriodEndDate -> reporting_period_end_date\n  dmicDataset -> dmic_dataset"
    )
}}
select
    "SK" as sk,
    "GroupSessId" as group_sess_id,
    "GroupSessDate" as group_sess_date,
    "OrgIDComm" as org_id_comm,
    "ClinContDurOfGroupSess" as clin_cont_dur_of_group_sess,
    "GroupSessType" as group_sess_type,
    "NumberOfGroupSessParticip" as number_of_group_sess_particip,
    "ActLocTypeCode" as act_loc_type_code,
    "SiteIDOfTreat" as site_id_of_treat,
    "CareProfLocalId" as care_prof_local_id,
    "ServTeamTypeRefToMH" as serv_team_type_ref_to_mh,
    "NHSServAgreeLineID" as nhs_serv_agree_line_id,
    "NHSServAgreeLineNum" as nhs_serv_agree_line_num,
    "MHS301UniqID" as mhs301_uniq_id,
    "OrgIDProv" as org_id_prov,
    "UniqSubmissionID" as uniq_submission_id,
    "UniqCareProfLocalID" as uniq_care_prof_local_id,
    "UniqMonthID" as uniq_month_id,
    "EFFECTIVE_FROM" as effective_from,
    "RowNumber" as row_number,
    "dmicImportLogId" as dmic_import_log_id,
    "UniqGroupSessId" as uniq_group_sess_id,
    "dmicSystemId" as dmic_system_id,
    "dmicCCGCode" as dmic_ccg_code,
    "dmicDateAdded" as dmic_date_added,
    "dmIcbCommissioner" as dm_icb_commissioner,
    "dmSubIcbCommissioner" as dm_sub_icb_commissioner,
    "dmCommissionerDerivationReason" as dm_commissioner_derivation_reason,
    "FileType" as file_type,
    "ReportingPeriodStartDate" as reporting_period_start_date,
    "ReportingPeriodEndDate" as reporting_period_end_date,
    "dmicDataset" as dmic_dataset
from {{ source('mhsds', 'MHS301GroupSession') }}
