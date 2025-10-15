-- Raw layer model for mhsds.MHS104RTT
-- Source: "DATA_LAKE"."MHSDS"
-- Description: Mental Health Services Data Set (MHSDS)
-- This is a 1:1 passthrough from source with standardized column names
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
