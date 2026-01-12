{{
    config(
        description="Raw layer (Primary care medications and prescribing data). 1:1 passthrough with cleaned column names. \nSource: DATA_LAKE.EPD_PRIMARY_CARE.MedsHeader \ndbt: source(''epd_primary_care'', ''MedsHeader'') \nColumns:\n  DatSerVer -> dat_ser_ver\n  OrgIdProvider -> org_id_provider\n  RPStartDate -> rp_start_date\n  RPEndDate -> rp_end_date\n  ReceivedDate -> received_date\n  FileType -> file_type\n  TotalRecords -> total_records\n  UniqSubmissionID -> uniq_submission_id\n  dmicProcessedPeriod -> dmic_processed_period\n  Unique_MonthID -> unique_month_id\n  dmicImportLogId -> dmic_import_log_id\n  dmicSystemId -> dmic_system_id\n  dmicDateAdded -> dmic_date_added"
    )
}}
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
