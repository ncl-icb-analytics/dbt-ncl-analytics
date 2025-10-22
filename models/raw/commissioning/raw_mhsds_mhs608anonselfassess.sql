-- Raw layer model for mhsds.MHS608AnonSelfAssess
-- Source: "DATA_LAKE"."MHSDS"
-- Description: Mental Health Services Data Set (MHSDS)
-- This is a 1:1 passthrough from source with standardized column names
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
