-- Raw layer model for mhsds.MHS505RestrictiveInterventInc
-- Source: "DATA_LAKE"."MHSDS"
-- Description: Mental Health Services Data Set (MHSDS)
-- This is a 1:1 passthrough from source with standardized column names
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
