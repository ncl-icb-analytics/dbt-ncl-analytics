{{
    config(
        description="Raw layer (Mental Health Services Data Set (MHSDS)). 1:1 passthrough with cleaned column names. \nSource: DATA_LAKE.MHSDS.MHS505RestrictiveIntervention \ndbt: source(''mhsds'', ''MHS505RestrictiveIntervention'') \nColumns:\n  SK -> sk\n  WardStayId -> ward_stay_id\n  StartDateRestrictiveInt -> start_date_restrictive_int\n  StartTimeRestrictiveInt -> start_time_restrictive_int\n  RestrictiveIntType -> restrictive_int_type\n  EndDateRestrictiveInt -> end_date_restrictive_int\n  EndTimeRestrictiveInt -> end_time_restrictive_int\n  RestraintInjuryPatient -> restraint_injury_patient\n  RestraintInjuryCarePers -> restraint_injury_care_pers\n  RestraintInjuryOtherPers -> restraint_injury_other_pers\n  RestrictiveIntPIReviewHeldPat -> restrictive_int_pi_review_held_pat\n  RestrictiveIntPIReviewNotHeldReasPat -> restrictive_int_pi_review_not_held_reas_pat\n  RestrictiveIntPIReviewHeldCarePers -> restrictive_int_pi_review_held_care_pers\n  RecordNumber -> record_number\n  MHS505UniqID -> mhs505_uniq_id\n  OrgIDProv -> org_id_prov\n  Person_ID -> person_id\n  UniqSubmissionID -> uniq_submission_id\n  UniqServReqID -> uniq_serv_req_id\n  UniqHospProvSpellNum -> uniq_hosp_prov_spell_num\n  UniqWardStayID -> uniq_ward_stay_id\n  UniqMonthID -> uniq_month_id\n  EFFECTIVE_FROM -> effective_from\n  dmicImportLogId -> dmic_import_log_id\n  dmicSystemId -> dmic_system_id\n  dmicCCGCode -> dmic_ccg_code\n  dmicDateAdded -> dmic_date_added\n  FileType -> file_type\n  ReportingPeriodStartDate -> reporting_period_start_date\n  ReportingPeriodEndDate -> reporting_period_end_date\n  dmicDataset -> dmic_dataset"
    )
}}
select
    "SK" as sk,
    "WardStayId" as ward_stay_id,
    "StartDateRestrictiveInt" as start_date_restrictive_int,
    "StartTimeRestrictiveInt" as start_time_restrictive_int,
    "RestrictiveIntType" as restrictive_int_type,
    "EndDateRestrictiveInt" as end_date_restrictive_int,
    "EndTimeRestrictiveInt" as end_time_restrictive_int,
    "RestraintInjuryPatient" as restraint_injury_patient,
    "RestraintInjuryCarePers" as restraint_injury_care_pers,
    "RestraintInjuryOtherPers" as restraint_injury_other_pers,
    "RestrictiveIntPIReviewHeldPat" as restrictive_int_pi_review_held_pat,
    "RestrictiveIntPIReviewNotHeldReasPat" as restrictive_int_pi_review_not_held_reas_pat,
    "RestrictiveIntPIReviewHeldCarePers" as restrictive_int_pi_review_held_care_pers,
    "RecordNumber" as record_number,
    "MHS505UniqID" as mhs505_uniq_id,
    "OrgIDProv" as org_id_prov,
    "Person_ID" as person_id,
    "UniqSubmissionID" as uniq_submission_id,
    "UniqServReqID" as uniq_serv_req_id,
    "UniqHospProvSpellNum" as uniq_hosp_prov_spell_num,
    "UniqWardStayID" as uniq_ward_stay_id,
    "UniqMonthID" as uniq_month_id,
    "EFFECTIVE_FROM" as effective_from,
    "dmicImportLogId" as dmic_import_log_id,
    "dmicSystemId" as dmic_system_id,
    "dmicCCGCode" as dmic_ccg_code,
    "dmicDateAdded" as dmic_date_added,
    "FileType" as file_type,
    "ReportingPeriodStartDate" as reporting_period_start_date,
    "ReportingPeriodEndDate" as reporting_period_end_date,
    "dmicDataset" as dmic_dataset
from {{ source('mhsds', 'MHS505RestrictiveIntervention') }}
