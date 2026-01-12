{{
    config(
        description="Raw layer (Mental Health Services Data Set (MHSDS)). 1:1 passthrough with cleaned column names. \nSource: DATA_LAKE.MHSDS.MHS606CodedScoreAssessmentRefer \ndbt: source(''mhsds'', ''MHS606CodedScoreAssessmentRefer'') \nColumns:\n  SK -> sk\n  ServiceRequestId -> service_request_id\n  CodedAssToolType -> coded_ass_tool_type\n  PersScore -> pers_score\n  AssToolCompTimestamp -> ass_tool_comp_timestamp\n  AssToolCompDate -> ass_tool_comp_date\n  CareProfLocalId -> care_prof_local_id\n  RecordNumber -> record_number\n  MHS606UniqID -> mhs606_uniq_id\n  OrgIDProv -> org_id_prov\n  Person_ID -> person_id\n  UniqSubmissionID -> uniq_submission_id\n  UniqServReqID -> uniq_serv_req_id\n  AgeAssessToolReferCompDate -> age_assess_tool_refer_comp_date\n  UniqCareProfLocalID -> uniq_care_prof_local_id\n  UniqMonthID -> uniq_month_id\n  EFFECTIVE_FROM -> effective_from\n  RowNumber -> row_number\n  dmicImportLogId -> dmic_import_log_id\n  dmicSystemId -> dmic_system_id\n  dmicCCGCode -> dmic_ccg_code\n  dmicDateAdded -> dmic_date_added\n  FileType -> file_type\n  ReportingPeriodStartDate -> reporting_period_start_date\n  ReportingPeriodEndDate -> reporting_period_end_date\n  dmicDataset -> dmic_dataset"
    )
}}
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
