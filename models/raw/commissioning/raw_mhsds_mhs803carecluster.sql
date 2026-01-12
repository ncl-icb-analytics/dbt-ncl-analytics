{{
    config(
        description="Raw layer (Mental Health Services Data Set (MHSDS)). 1:1 passthrough with cleaned column names. \nSource: DATA_LAKE.MHSDS.MHS803CareCluster \ndbt: source(''mhsds'', ''MHS803CareCluster'') \nColumns:\n  SK -> sk\n  ClustId -> clust_id\n  StartDateCareClust -> start_date_care_clust\n  StartTimeCareClust -> start_time_care_clust\n  AMHCareClustCodeFin -> amh_care_clust_code_fin\n  CAMHNeedsBasedGroupingCode -> camh_needs_based_grouping_code\n  LDCareClustCodeFin -> ld_care_clust_code_fin\n  FMHCareClustCodeFin -> fmh_care_clust_code_fin\n  FLDCareClustCodeFin -> fld_care_clust_code_fin\n  EndDateCareClust -> end_date_care_clust\n  EndTimeCareClust -> end_time_care_clust\n  RecordNumber -> record_number\n  MHS803UniqID -> mhs803_uniq_id\n  OrgIDProv -> org_id_prov\n  Person_ID -> person_id\n  UniqSubmissionID -> uniq_submission_id\n  UniqClustID -> uniq_clust_id\n  ClusterStartRPFlag -> cluster_start_rp_flag\n  ClusterEndRPFlag -> cluster_end_rp_flag\n  ClusterOpenEndRPFlag -> cluster_open_end_rp_flag\n  ClusterDaysRP -> cluster_days_rp\n  UniqMonthID -> uniq_month_id\n  RecordStartDate -> record_start_date\n  RecordEndDate -> record_end_date\n  InactTimeCC -> inact_time_cc\n  EFFECTIVE_FROM -> effective_from\n  dmicImportLogId -> dmic_import_log_id\n  dmicSystemId -> dmic_system_id\n  dmicCCGCode -> dmic_ccg_code\n  dmicDateAdded -> dmic_date_added\n  FileType -> file_type\n  ReportingPeriodStartDate -> reporting_period_start_date\n  ReportingPeriodEndDate -> reporting_period_end_date\n  dmicDataset -> dmic_dataset"
    )
}}
select
    "SK" as sk,
    "ClustId" as clust_id,
    "StartDateCareClust" as start_date_care_clust,
    "StartTimeCareClust" as start_time_care_clust,
    "AMHCareClustCodeFin" as amh_care_clust_code_fin,
    "CAMHNeedsBasedGroupingCode" as camh_needs_based_grouping_code,
    "LDCareClustCodeFin" as ld_care_clust_code_fin,
    "FMHCareClustCodeFin" as fmh_care_clust_code_fin,
    "FLDCareClustCodeFin" as fld_care_clust_code_fin,
    "EndDateCareClust" as end_date_care_clust,
    "EndTimeCareClust" as end_time_care_clust,
    "RecordNumber" as record_number,
    "MHS803UniqID" as mhs803_uniq_id,
    "OrgIDProv" as org_id_prov,
    "Person_ID" as person_id,
    "UniqSubmissionID" as uniq_submission_id,
    "UniqClustID" as uniq_clust_id,
    "ClusterStartRPFlag" as cluster_start_rp_flag,
    "ClusterEndRPFlag" as cluster_end_rp_flag,
    "ClusterOpenEndRPFlag" as cluster_open_end_rp_flag,
    "ClusterDaysRP" as cluster_days_rp,
    "UniqMonthID" as uniq_month_id,
    "RecordStartDate" as record_start_date,
    "RecordEndDate" as record_end_date,
    "InactTimeCC" as inact_time_cc,
    "EFFECTIVE_FROM" as effective_from,
    "dmicImportLogId" as dmic_import_log_id,
    "dmicSystemId" as dmic_system_id,
    "dmicCCGCode" as dmic_ccg_code,
    "dmicDateAdded" as dmic_date_added,
    "FileType" as file_type,
    "ReportingPeriodStartDate" as reporting_period_start_date,
    "ReportingPeriodEndDate" as reporting_period_end_date,
    "dmicDataset" as dmic_dataset
from {{ source('mhsds', 'MHS803CareCluster') }}
