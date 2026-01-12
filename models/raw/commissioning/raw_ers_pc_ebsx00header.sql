{{
    config(
        description="Raw layer (Primary care referrals data). 1:1 passthrough with cleaned column names. \nSource: DATA_LAKE.ERS.ebsx00Header \ndbt: source(''eRS_primary_care'', ''ebsx00Header'') \nColumns:\n  Version -> version\n  OrgID_Referrer -> org_id_referrer\n  UniqSubmissionID -> uniq_submission_id\n  ebsx00_ID -> ebsx00_id\n  File_Type -> file_type\n  RP_StartDate -> rp_start_date\n  RP_EndDate -> rp_end_date\n  Unique_MonthID -> unique_month_id\n  Total_ebsx02 -> total_ebsx02\n  TotalRecords -> total_records\n  dmicImportLogId -> dmic_import_log_id\n  dmicMonthId -> dmic_month_id\n  dmicSystemId -> dmic_system_id\n  dmicCCGCodeReferrer -> dmic_ccg_code_referrer\n  dmicDSCRO -> dmic_dscro\n  dmicDateAdded -> dmic_date_added"
    )
}}
select
    "Version" as version,
    "OrgID_Referrer" as org_id_referrer,
    "UniqSubmissionID" as uniq_submission_id,
    "ebsx00_ID" as ebsx00_id,
    "File_Type" as file_type,
    "RP_StartDate" as rp_start_date,
    "RP_EndDate" as rp_end_date,
    "Unique_MonthID" as unique_month_id,
    "Total_ebsx02" as total_ebsx02,
    "TotalRecords" as total_records,
    "dmicImportLogId" as dmic_import_log_id,
    "dmicMonthId" as dmic_month_id,
    "dmicSystemId" as dmic_system_id,
    "dmicCCGCodeReferrer" as dmic_ccg_code_referrer,
    "dmicDSCRO" as dmic_dscro,
    "dmicDateAdded" as dmic_date_added
from {{ source('eRS_primary_care', 'ebsx00Header') }}
