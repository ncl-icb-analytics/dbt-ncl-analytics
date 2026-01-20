{{
    config(
        description="Raw layer (Mental Health Services Data Set (MHSDS)). 1:1 passthrough with cleaned column names. \nSource: DATA_LAKE.MHSDS.MHS104RTT \ndbt: source(''mhsds'', ''MHS104RTT'') \nColumns:\n  SK -> sk\n  ServiceRequestId -> service_request_id\n  PatPathId Pseudo -> pat_path_id_pseudo\n  WaitTimeMeasureType -> wait_time_measure_type\n  OrgIDPatPathIdIssuer -> org_id_pat_path_id_issuer\n  ReferToTreatPeriodStartDate -> refer_to_treat_period_start_date\n  ReferToTreatPeriodEndDate -> refer_to_treat_period_end_date\n  ReferToTreatPeriodStatus -> refer_to_treat_period_status\n  RecordNumber -> record_number\n  MHS104UniqID -> mhs104_uniq_id\n  OrgIDProv -> org_id_prov\n  Person_ID -> person_id\n  UniqSubmissionID -> uniq_submission_id\n  UniqServReqID -> uniq_serv_req_id\n  AgeReferTreatStartDate -> age_refer_treat_start_date\n  AgeReferTreatEndDate -> age_refer_treat_end_date\n  TimeReferStartAndEndDate -> time_refer_start_and_end_date\n  UniqMonthID -> uniq_month_id\n  EFFECTIVE_FROM -> effective_from\n  RowNumber -> row_number\n  dmicImportLogId -> dmic_import_log_id\n  dmicSystemId -> dmic_system_id\n  dmicCCGCode -> dmic_ccg_code\n  dmicDateAdded -> dmic_date_added\n  FileType -> file_type\n  ReportingPeriodStartDate -> reporting_period_start_date\n  ReportingPeriodEndDate -> reporting_period_end_date\n  dmicDataset -> dmic_dataset"
    )
}}
select
    "SK" as sk,
    "ServiceRequestId" as service_request_id,
    "PatPathId Pseudo" as pat_path_id_pseudo,
    "WaitTimeMeasureType" as wait_time_measure_type,
    "OrgIDPatPathIdIssuer" as org_id_pat_path_id_issuer,
    "ReferToTreatPeriodStartDate" as refer_to_treat_period_start_date,
    "ReferToTreatPeriodEndDate" as refer_to_treat_period_end_date,
    "ReferToTreatPeriodStatus" as refer_to_treat_period_status,
    "RecordNumber" as record_number,
    "MHS104UniqID" as mhs104_uniq_id,
    "OrgIDProv" as org_id_prov,
    "Person_ID" as person_id,
    "UniqSubmissionID" as uniq_submission_id,
    "UniqServReqID" as uniq_serv_req_id,
    "AgeReferTreatStartDate" as age_refer_treat_start_date,
    "AgeReferTreatEndDate" as age_refer_treat_end_date,
    "TimeReferStartAndEndDate" as time_refer_start_and_end_date,
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
from {{ source('mhsds', 'MHS104RTT') }}
