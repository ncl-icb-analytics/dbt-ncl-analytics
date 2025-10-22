-- Raw layer model for mhsds.MHS505RestrictiveIntervention
-- Source: "DATA_LAKE"."MHSDS"
-- Description: Mental Health Services Data Set (MHSDS)
-- This is a 1:1 passthrough from source with standardized column names
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
