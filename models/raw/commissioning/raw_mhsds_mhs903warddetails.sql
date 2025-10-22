-- Raw layer model for mhsds.MHS903WardDetails
-- Source: "DATA_LAKE"."MHSDS"
-- Description: Mental Health Services Data Set (MHSDS)
-- This is a 1:1 passthrough from source with standardized column names
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
