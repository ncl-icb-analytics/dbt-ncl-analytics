-- Raw layer model for mhsds.MHS008CarePlanType
-- Source: "DATA_LAKE"."MHSDS"
-- Description: Mental Health Services Data Set (MHSDS)
-- This is a 1:1 passthrough from source with standardized column names
select
    "SK" as sk,
    "CarePlanID" as care_plan_id,
    "LocalPatientId" as local_patient_id,
    "CarePlanTypeMH" as care_plan_type_mh,
    "CarePlanCreatDate" as care_plan_creat_date,
    "CarePlanCreationTime" as care_plan_creation_time,
    "CarePlanLastUpdateDate" as care_plan_last_update_date,
    "CarePlanLastUpdateTime" as care_plan_last_update_time,
    "CarePlanImplementDate" as care_plan_implement_date,
    "RecordNumber" as record_number,
    "MHS008UniqID" as mhs008_uniq_id,
    "OrgIDProv" as org_id_prov,
    "Person_ID" as person_id,
    "UniqSubmissionID" as uniq_submission_id,
    "UniqCarePlanID" as uniq_care_plan_id,
    "UniqMonthID" as uniq_month_id,
    "RecordStartDate" as record_start_date,
    "RecordEndDate" as record_end_date,
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
from {{ source('mhsds', 'MHS008CarePlanType') }}
