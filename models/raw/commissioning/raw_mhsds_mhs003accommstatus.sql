{{
    config(
        description="Raw layer (Mental Health Services Data Set (MHSDS)). 1:1 passthrough with cleaned column names. \nSource: DATA_LAKE.MHSDS.MHS003AccommStatus \ndbt: source(''mhsds'', ''MHS003AccommStatus'') \nColumns:\n  SK -> sk\n  LocalPatientId -> local_patient_id\n  AccommodationStatusCode -> accommodation_status_code\n  AccommodationType -> accommodation_type\n  SettledAccommodationInd -> settled_accommodation_ind\n  AccommodationStatusDate -> accommodation_status_date\n  AccommodationTypeDate -> accommodation_type_date\n  SCHPlacementType -> sch_placement_type\n  AccommodationTypeStartDate -> accommodation_type_start_date\n  AccommodationTypeEndDate -> accommodation_type_end_date\n  RecordNumber -> record_number\n  MHS003UniqID -> mhs003_uniq_id\n  OrgIDProv -> org_id_prov\n  Person_ID -> person_id\n  UniqSubmissionID -> uniq_submission_id\n  AgeAccomStatusDate -> age_accom_status_date\n  AgeAccomTypeDate -> age_accom_type_date\n  UniqMonthID -> uniq_month_id\n  EFFECTIVE_FROM -> effective_from\n  RowNumber -> row_number\n  dmicImportLogId -> dmic_import_log_id\n  dmicSystemId -> dmic_system_id\n  dmicCCGCode -> dmic_ccg_code\n  dmicDateAdded -> dmic_date_added\n  Unique_LocalPatientId -> unique_local_patient_id\n  FileType -> file_type\n  ReportingPeriodStartDate -> reporting_period_start_date\n  ReportingPeriodEndDate -> reporting_period_end_date\n  dmicDataset -> dmic_dataset"
    )
}}
select
    "SK" as sk,
    "LocalPatientId" as local_patient_id,
    "AccommodationStatusCode" as accommodation_status_code,
    "AccommodationType" as accommodation_type,
    "SettledAccommodationInd" as settled_accommodation_ind,
    "AccommodationStatusDate" as accommodation_status_date,
    "AccommodationTypeDate" as accommodation_type_date,
    "SCHPlacementType" as sch_placement_type,
    "AccommodationTypeStartDate" as accommodation_type_start_date,
    "AccommodationTypeEndDate" as accommodation_type_end_date,
    "RecordNumber" as record_number,
    "MHS003UniqID" as mhs003_uniq_id,
    "OrgIDProv" as org_id_prov,
    "Person_ID" as person_id,
    "UniqSubmissionID" as uniq_submission_id,
    "AgeAccomStatusDate" as age_accom_status_date,
    "AgeAccomTypeDate" as age_accom_type_date,
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
from {{ source('mhsds', 'MHS003AccommStatus') }}
