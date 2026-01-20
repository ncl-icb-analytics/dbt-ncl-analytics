{{
    config(
        description="Raw layer (Mental Health Services Data Set (MHSDS)). 1:1 passthrough with cleaned column names. \nSource: DATA_LAKE.MHSDS.MHS608AnonSelfAssess \ndbt: source(''mhsds'', ''MHS608AnonSelfAssess'') \nColumns:\n  AssToolCompTimestamp -> ass_tool_comp_timestamp\n  AssToolCompDate -> ass_tool_comp_date\n  CodedAssToolType -> coded_ass_tool_type\n  PersScore -> pers_score\n  ActLocTypeCode -> act_loc_type_code\n  OrgIDComm -> org_id_comm\n  MHS608UniqID -> mhs608_uniq_id\n  OrgIDProv -> org_id_prov\n  UniqSubmissionID -> uniq_submission_id\n  UniqMonthID -> uniq_month_id\n  EFFECTIVE_FROM -> effective_from\n  RowNumber -> row_number\n  dmicImportLogId -> dmic_import_log_id\n  dmicSystemId -> dmic_system_id\n  dmicCCGCode -> dmic_ccg_code\n  dmicDateAdded -> dmic_date_added\n  FileType -> file_type\n  ReportingPeriodStartDate -> reporting_period_start_date\n  ReportingPeriodEndDate -> reporting_period_end_date\n  dmicDataset -> dmic_dataset"
    )
}}
select
    "AssToolCompTimestamp" as ass_tool_comp_timestamp,
    "AssToolCompDate" as ass_tool_comp_date,
    "CodedAssToolType" as coded_ass_tool_type,
    "PersScore" as pers_score,
    "ActLocTypeCode" as act_loc_type_code,
    "OrgIDComm" as org_id_comm,
    "MHS608UniqID" as mhs608_uniq_id,
    "OrgIDProv" as org_id_prov,
    "UniqSubmissionID" as uniq_submission_id,
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
from {{ source('mhsds', 'MHS608AnonSelfAssess') }}
