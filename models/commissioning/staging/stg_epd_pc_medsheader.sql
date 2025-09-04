-- Staging model for epd_primary_care.MedsHeader
-- Source: "DATA_LAKE"."EPD_PRIMARY_CARE"
-- Description: Primary care medications and prescribing data

select
    "DatSerVer" as dat_ser_ver,
    "OrgIdProvider" as org_id_provider,
    "RPStartDate" as rp_start_date,
    "RPEndDate" as rp_end_date,
    "ReceivedDate" as received_date,
    "FileType" as file_type,
    "TotalRecords" as total_records,
    "UniqSubmissionID" as uniq_submission_id,
    "dmicProcessedPeriod" as dmic_processed_period,
    "Unique_MonthID" as unique_month_id,
    "dmicImportLogId" as dmic_import_log_id,
    "dmicSystemId" as dmic_system_id,
    "dmicDateAdded" as dmic_date_added
from {{ source('epd_primary_care', 'MedsHeader') }}
