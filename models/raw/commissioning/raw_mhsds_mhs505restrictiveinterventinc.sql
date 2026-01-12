{{
    config(
        description="Raw layer (Mental Health Services Data Set (MHSDS)). 1:1 passthrough with cleaned column names. \nSource: DATA_LAKE.MHSDS.MHS505RestrictiveInterventInc \ndbt: source(''mhsds'', ''MHS505RestrictiveInterventInc'') \nColumns:\n  SK -> sk\n  RestrictiveIntIncID -> restrictive_int_inc_id\n  HospProvSpellID -> hosp_prov_spell_id\n  StartDateRestrictiveIntInc -> start_date_restrictive_int_inc\n  StartTimeRestrictiveIntInc -> start_time_restrictive_int_inc\n  EndDateRestrictiveIntInc -> end_date_restrictive_int_inc\n  EndTimeRestrictiveIntInc -> end_time_restrictive_int_inc\n  RestrictiveIntReason -> restrictive_int_reason\n  RestrictiveIntPIReviewHeldPat -> restrictive_int_pi_review_held_pat\n  RestrictiveIntPIReviewNotHeldReasPat -> restrictive_int_pi_review_not_held_reas_pat\n  RestrictiveIntPIReviewHeldCarePers -> restrictive_int_pi_review_held_care_pers\n  RecordNumber -> record_number\n  MHS505UniqID -> mhs505_uniq_id\n  OrgIDProv -> org_id_prov\n  Person_ID -> person_id\n  UniqSubmissionID -> uniq_submission_id\n  UniqServReqID -> uniq_serv_req_id\n  UniqHospProvSpellID -> uniq_hosp_prov_spell_id\n  UniqMonthID -> uniq_month_id\n  EFFECTIVE_FROM -> effective_from\n  RecordStartDate -> record_start_date\n  RecordEndDate -> record_end_date\n  UniqRestrictiveIntIncID -> uniq_restrictive_int_inc_id\n  RowNumber -> row_number\n  dmicImportLogId -> dmic_import_log_id\n  dmicSystemId -> dmic_system_id\n  dmicCCGCode -> dmic_ccg_code\n  dmicDateAdded -> dmic_date_added\n  FileType -> file_type\n  ReportingPeriodStartDate -> reporting_period_start_date\n  ReportingPeriodEndDate -> reporting_period_end_date\n  dmicDataset -> dmic_dataset"
    )
}}
select
    "SK" as sk,
    "RestrictiveIntIncID" as restrictive_int_inc_id,
    "HospProvSpellID" as hosp_prov_spell_id,
    "StartDateRestrictiveIntInc" as start_date_restrictive_int_inc,
    "StartTimeRestrictiveIntInc" as start_time_restrictive_int_inc,
    "EndDateRestrictiveIntInc" as end_date_restrictive_int_inc,
    "EndTimeRestrictiveIntInc" as end_time_restrictive_int_inc,
    "RestrictiveIntReason" as restrictive_int_reason,
    "RestrictiveIntPIReviewHeldPat" as restrictive_int_pi_review_held_pat,
    "RestrictiveIntPIReviewNotHeldReasPat" as restrictive_int_pi_review_not_held_reas_pat,
    "RestrictiveIntPIReviewHeldCarePers" as restrictive_int_pi_review_held_care_pers,
    "RecordNumber" as record_number,
    "MHS505UniqID" as mhs505_uniq_id,
    "OrgIDProv" as org_id_prov,
    "Person_ID" as person_id,
    "UniqSubmissionID" as uniq_submission_id,
    "UniqServReqID" as uniq_serv_req_id,
    "UniqHospProvSpellID" as uniq_hosp_prov_spell_id,
    "UniqMonthID" as uniq_month_id,
    "EFFECTIVE_FROM" as effective_from,
    "RecordStartDate" as record_start_date,
    "RecordEndDate" as record_end_date,
    "UniqRestrictiveIntIncID" as uniq_restrictive_int_inc_id,
    "RowNumber" as row_number,
    "dmicImportLogId" as dmic_import_log_id,
    "dmicSystemId" as dmic_system_id,
    "dmicCCGCode" as dmic_ccg_code,
    "dmicDateAdded" as dmic_date_added,
    "FileType" as file_type,
    "ReportingPeriodStartDate" as reporting_period_start_date,
    "ReportingPeriodEndDate" as reporting_period_end_date,
    "dmicDataset" as dmic_dataset
from {{ source('mhsds', 'MHS505RestrictiveInterventInc') }}
