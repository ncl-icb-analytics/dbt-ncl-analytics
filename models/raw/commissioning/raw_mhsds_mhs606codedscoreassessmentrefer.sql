-- Raw layer model for mhsds.MHS606CodedScoreAssessmentRefer
-- Source: "DATA_LAKE"."MHSDS"
-- Description: Mental Health Services Data Set (MHSDS)
-- This is a 1:1 passthrough from source with standardized column names
select
    "SK" as sk,
    "ServiceRequestId" as service_request_id,
    "CodedAssToolType" as coded_ass_tool_type,
    "PersScore" as pers_score,
    "AssToolCompTimestamp" as ass_tool_comp_timestamp,
    "AssToolCompDate" as ass_tool_comp_date,
    "CareProfLocalId" as care_prof_local_id,
    "RecordNumber" as record_number,
    "MHS606UniqID" as mhs606_uniq_id,
    "OrgIDProv" as org_id_prov,
    "Person_ID" as person_id,
    "UniqSubmissionID" as uniq_submission_id,
    "UniqServReqID" as uniq_serv_req_id,
    "AgeAssessToolReferCompDate" as age_assess_tool_refer_comp_date,
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
from {{ source('mhsds', 'MHS606CodedScoreAssessmentRefer') }}
