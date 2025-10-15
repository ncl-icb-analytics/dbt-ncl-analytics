-- Raw layer model for mhsds.MHS801ClusterTool
-- Source: "DATA_LAKE"."MHSDS"
-- Description: Mental Health Services Data Set (MHSDS)
-- This is a 1:1 passthrough from source with standardized column names
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
