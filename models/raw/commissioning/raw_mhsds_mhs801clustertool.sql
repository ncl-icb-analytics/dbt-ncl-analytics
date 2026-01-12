{{
    config(
        description="Raw layer (Mental Health Services Data Set (MHSDS)). 1:1 passthrough with cleaned column names. \nSource: DATA_LAKE.MHSDS.MHS801ClusterTool \ndbt: source(''mhsds'', ''MHS801ClusterTool'') \nColumns:\n  SK -> sk\n  ClustId -> clust_id\n  LocalPatientId -> local_patient_id\n  ClustCat -> clust_cat\n  AssToolCompDate -> ass_tool_comp_date\n  AssToolCompTime -> ass_tool_comp_time\n  ClustToolAssReason -> clust_tool_ass_reason\n  MHCareClusterSuperClass -> mh_care_cluster_super_class\n  AMHCareClustCodeInit -> amh_care_clust_code_init\n  LDCareClustInit -> ld_care_clust_init\n  FLDCareClustInit -> fld_care_clust_init\n  RecordNumber -> record_number\n  MHS801UniqID -> mhs801_uniq_id\n  OrgIDProv -> org_id_prov\n  Person_ID -> person_id\n  UniqSubmissionID -> uniq_submission_id\n  UniqClustID -> uniq_clust_id\n  UniqMonthID -> uniq_month_id\n  EFFECTIVE_FROM -> effective_from\n  dmicImportLogId -> dmic_import_log_id\n  dmicSystemId -> dmic_system_id\n  dmicCCGCode -> dmic_ccg_code\n  dmicDateAdded -> dmic_date_added\n  Unique_LocalPatientId -> unique_local_patient_id\n  FileType -> file_type\n  ReportingPeriodStartDate -> reporting_period_start_date\n  ReportingPeriodEndDate -> reporting_period_end_date\n  dmicDataset -> dmic_dataset"
    )
}}
select
    "SK" as sk,
    "ClustId" as clust_id,
    "LocalPatientId" as local_patient_id,
    "ClustCat" as clust_cat,
    "AssToolCompDate" as ass_tool_comp_date,
    "AssToolCompTime" as ass_tool_comp_time,
    "ClustToolAssReason" as clust_tool_ass_reason,
    "MHCareClusterSuperClass" as mh_care_cluster_super_class,
    "AMHCareClustCodeInit" as amh_care_clust_code_init,
    "LDCareClustInit" as ld_care_clust_init,
    "FLDCareClustInit" as fld_care_clust_init,
    "RecordNumber" as record_number,
    "MHS801UniqID" as mhs801_uniq_id,
    "OrgIDProv" as org_id_prov,
    "Person_ID" as person_id,
    "UniqSubmissionID" as uniq_submission_id,
    "UniqClustID" as uniq_clust_id,
    "UniqMonthID" as uniq_month_id,
    "EFFECTIVE_FROM" as effective_from,
    "dmicImportLogId" as dmic_import_log_id,
    "dmicSystemId" as dmic_system_id,
    "dmicCCGCode" as dmic_ccg_code,
    "dmicDateAdded" as dmic_date_added,
    "Unique_LocalPatientId" as unique_local_patient_id,
    "FileType" as file_type,
    "ReportingPeriodStartDate" as reporting_period_start_date,
    "ReportingPeriodEndDate" as reporting_period_end_date,
    "dmicDataset" as dmic_dataset
from {{ source('mhsds', 'MHS801ClusterTool') }}
