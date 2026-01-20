{{
    config(
        description="Raw layer (Mental Health Services Data Set (MHSDS)). 1:1 passthrough with cleaned column names. \nSource: DATA_LAKE.MHSDS.MHS702CPAReview \ndbt: source(''mhsds'', ''MHS702CPAReview'') \nColumns:\n  SK -> sk\n  CPAEpisodeId -> cpa_episode_id\n  CPAReviewDate -> cpa_review_date\n  CPARevAbuseQuestAskInd -> cpa_rev_abuse_quest_ask_ind\n  CareProfLocalId -> care_prof_local_id\n  RecordNumber -> record_number\n  MHS702UniqID -> mhs702_uniq_id\n  OrgIDProv -> org_id_prov\n  Person_ID -> person_id\n  UniqSubmissionID -> uniq_submission_id\n  UniqCPAEpisodeID -> uniq_cpa_episode_id\n  UniqCareProfLocalID -> uniq_care_prof_local_id\n  UniqMonthID -> uniq_month_id\n  EFFECTIVE_FROM -> effective_from\n  RowNumber -> row_number\n  dmicImportLogId -> dmic_import_log_id\n  dmicSystemId -> dmic_system_id\n  dmicCCGCode -> dmic_ccg_code\n  dmicDateAdded -> dmic_date_added\n  FileType -> file_type\n  ReportingPeriodStartDate -> reporting_period_start_date\n  ReportingPeriodEndDate -> reporting_period_end_date\n  dmicDataset -> dmic_dataset"
    )
}}
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
