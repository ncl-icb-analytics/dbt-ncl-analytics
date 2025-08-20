-- Staging model for eRS_primary_care.ebsx00Header
-- Source: "DATA_LAKE"."ERS"
-- Description: Primary care referrals data

select
    "Version" as version,
    "OrgID_Referrer" as orgid_referrer,
    "UniqSubmissionID" as uniqsubmissionid,
    "ebsx00_ID" as ebsx00_id,
    "File_Type" as file_type,
    "RP_StartDate" as rp_startdate,
    "RP_EndDate" as rp_enddate,
    "Unique_MonthID" as unique_monthid,
    "Total_ebsx02" as total_ebsx02,
    "TotalRecords" as totalrecords,
    "dmicImportLogId" as dmicimportlogid,
    "dmicMonthId" as dmicmonthid,
    "dmicSystemId" as dmicsystemid,
    "dmicCCGCodeReferrer" as dmicccgcodereferrer,
    "dmicDSCRO" as dmicdscro,
    "dmicDateAdded" as dmicdateadded
from {{ source('eRS_primary_care', 'ebsx00Header') }}
