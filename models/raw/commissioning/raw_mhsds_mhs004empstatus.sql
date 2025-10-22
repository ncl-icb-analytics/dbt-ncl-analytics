-- Raw layer model for mhsds.MHS004EmpStatus
-- Source: "DATA_LAKE"."MHSDS"
-- Description: Mental Health Services Data Set (MHSDS)
-- This is a 1:1 passthrough from source with standardized column names
select
    "SK" as sk,
    "LocalPatientId" as local_patient_id,
    "EmployStatus" as employ_status,
    "EmployStatusStartDate" as employ_status_start_date,
    "EmployStatusEndDate" as employ_status_end_date,
    "EmployStatusRecDate" as employ_status_rec_date,
    "PatPrimEmpContTypeMH" as pat_prim_emp_cont_type_mh,
    "WeekHoursWorked" as week_hours_worked,
    "RecordNumber" as record_number,
    "MHS004UniqID" as mhs004_uniq_id,
    "OrgIDProv" as org_id_prov,
    "Person_ID" as person_id,
    "UniqSubmissionID" as uniq_submission_id,
    "UniqMonthID" as uniq_month_id,
    "EFFECTIVE_FROM" as effective_from,
    "RowNumber" as row_number,
    "dmicImportLogId" as dmic_import_log_id,
    "dmicSystemId" as dmic_system_id,
    "dmicCCGCode" as dmic_ccg_code,
    "dmicDateAdded" as dmic_date_added,
    "Unique_LocalPatientId" as unique_local_patient_id,
    "FileType" as file_type,
    "ReportingPeriodStartDate" as reporting_period_start_date,
    "ReportingPeriodEndDate" as reporting_period_end_date,
    "dmicDataset" as dmic_dataset
from {{ source('mhsds', 'MHS004EmpStatus') }}
