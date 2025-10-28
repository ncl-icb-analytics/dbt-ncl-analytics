-- Raw layer model for mhsds.MHS702CPAReview
-- Source: "DATA_LAKE"."MHSDS"
-- Description: Mental Health Services Data Set (MHSDS)
-- This is a 1:1 passthrough from source with standardized column names
select
    "SK" as sk,
    "CPAEpisodeId" as cpa_episode_id,
    "CPAReviewDate" as cpa_review_date,
    "CPARevAbuseQuestAskInd" as cpa_rev_abuse_quest_ask_ind,
    "CareProfLocalId" as care_prof_local_id,
    "RecordNumber" as record_number,
    "MHS702UniqID" as mhs702_uniq_id,
    "OrgIDProv" as org_id_prov,
    "Person_ID" as person_id,
    "UniqSubmissionID" as uniq_submission_id,
    "UniqCPAEpisodeID" as uniq_cpa_episode_id,
    "UniqCareProfLocalID" as uniq_care_prof_local_id,
    "UniqMonthID" as uniq_month_id,
    "EFFECTIVE_FROM" as effective_from,
    "RowNumber" as row_number,
    "dmicImportLogId" as dmic_import_log_id,
    "dmicSystemId" as dmic_system_id,
    "dmicCCGCode" as dmic_ccg_code,
    "dmicDateAdded" as dmic_date_added,
    "FileType" as file_type,
    "ReportingPeriodStartDate" as reporting_period_start_date,
    "ReportingPeriodEndDate" as reporting_period_end_date,
    "dmicDataset" as dmic_dataset
from {{ source('mhsds', 'MHS702CPAReview') }}
