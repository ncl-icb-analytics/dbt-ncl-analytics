-- Raw layer model for mhsds.MHS013MHCurrencyModel
-- Source: "DATA_LAKE"."MHSDS"
-- Description: Mental Health Services Data Set (MHSDS)
-- This is a 1:1 passthrough from source with standardized column names
select
    "SK" as sk,
    "LocalPatientId" as local_patient_id,
    "MHResourceGroupType" as mh_resource_group_type,
    "StartDateMHResourceGroup" as start_date_mh_resource_group,
    "EndDateMHResourceGroup" as end_date_mh_resource_group,
    "RecordNumber" as record_number,
    "MHS013UniqID" as mhs013_uniq_id,
    "OrgIDProv" as org_id_prov,
    "Person_ID" as person_id,
    "UniqSubmissionID" as uniq_submission_id,
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
from {{ source('mhsds', 'MHS013MHCurrencyModel') }}
