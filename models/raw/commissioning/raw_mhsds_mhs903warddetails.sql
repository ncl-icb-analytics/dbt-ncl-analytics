{{
    config(
        description="Raw layer (Mental Health Services Data Set (MHSDS)). 1:1 passthrough with cleaned column names. \nSource: DATA_LAKE.MHSDS.MHS903WardDetails \ndbt: source(''mhsds'', ''MHS903WardDetails'') \nColumns:\n  WardCode -> ward_code\n  SiteIDOfWard -> site_id_of_ward\n  WardIntendedSex -> ward_intended_sex\n  WardIntendedClinCareMH -> ward_intended_clin_care_mh\n  WardAge -> ward_age\n  WardType -> ward_type\n  WardSecLevel -> ward_sec_level\n  LockedWardInd -> locked_ward_ind\n  AvailBedDays -> avail_bed_days\n  ClosedBedDays -> closed_bed_days\n  MHS903UniqID -> mhs903_uniq_id\n  OrgIDProv -> org_id_prov\n  UniqSubmissionID -> uniq_submission_id\n  UniqMonthID -> uniq_month_id\n  EFFECTIVE_FROM -> effective_from\n  UniqWardCode -> uniq_ward_code\n  RowNumber -> row_number\n  dmicImportLogId -> dmic_import_log_id\n  dmicSystemId -> dmic_system_id\n  dmicCCGCode -> dmic_ccg_code\n  dmicDateAdded -> dmic_date_added\n  FileType -> file_type\n  ReportingPeriodStartDate -> reporting_period_start_date\n  ReportingPeriodEndDate -> reporting_period_end_date\n  dmicDataset -> dmic_dataset"
    )
}}
select
    "WardCode" as ward_code,
    "SiteIDOfWard" as site_id_of_ward,
    "WardIntendedSex" as ward_intended_sex,
    "WardIntendedClinCareMH" as ward_intended_clin_care_mh,
    "WardAge" as ward_age,
    "WardType" as ward_type,
    "WardSecLevel" as ward_sec_level,
    "LockedWardInd" as locked_ward_ind,
    "AvailBedDays" as avail_bed_days,
    "ClosedBedDays" as closed_bed_days,
    "MHS903UniqID" as mhs903_uniq_id,
    "OrgIDProv" as org_id_prov,
    "UniqSubmissionID" as uniq_submission_id,
    "UniqMonthID" as uniq_month_id,
    "EFFECTIVE_FROM" as effective_from,
    "UniqWardCode" as uniq_ward_code,
    "RowNumber" as row_number,
    "dmicImportLogId" as dmic_import_log_id,
    "dmicSystemId" as dmic_system_id,
    "dmicCCGCode" as dmic_ccg_code,
    "dmicDateAdded" as dmic_date_added,
    "FileType" as file_type,
    "ReportingPeriodStartDate" as reporting_period_start_date,
    "ReportingPeriodEndDate" as reporting_period_end_date,
    "dmicDataset" as dmic_dataset
from {{ source('mhsds', 'MHS903WardDetails') }}
