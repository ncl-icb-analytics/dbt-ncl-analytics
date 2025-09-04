-- Staging model for eRS_primary_care.ebsx00Header
-- Source: "DATA_LAKE"."ERS"
-- Description: Primary care referrals data

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
