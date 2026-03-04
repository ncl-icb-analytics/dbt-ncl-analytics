{{
    config(
        description="Raw layer (ptl data). 1:1 passthrough with cleaned column names. \nSource: DATA_LAKE__NCL.PTL.PTL_WEEKLY_UPLOAD_WITH_ROW_IDS \ndbt: source(''ptl'', ''PTL_WEEKLY_UPLOAD_WITH_ROW_IDS'') \nColumns:\n  CENSUS_DATE -> census_date\n  PROVIDER_CODE -> provider_code\n  PROVIDER_SITE -> provider_site\n  COUNT -> count\n  CCG_CODE -> ccg_code\n  GP_PRACTICE_CODE -> gp_practice_code\n  GENDER -> gender\n  ETHNICITY -> ethnicity\n  AGE -> age\n  REFERRAL_RECEIVED_DATE -> referral_received_date\n  REFERRAL_SOURCE -> referral_source\n  REFERRING_ORG_CODE -> referring_org_code\n  IS_EREFERRAL -> is_ereferral\n  ASI_FLAG -> asi_flag\n  RTT_WAIT_DAYS -> rtt_wait_days\n  ADMITTED_NONADMITTED_LIST -> admitted_nonadmitted_list\n  HAS_HAD_FIRST_OP -> has_had_first_op\n  PATIENT_REFUSAL_DEFERRAL_DATE -> patient_refusal_deferral_date\n  TCI_STATUS -> tci_status\n  TCI_DECISION_DATE -> tci_decision_date\n  TCI_DATE -> tci_date\n  CLINICAL_PRIORITY -> clinical_priority\n  CLINICAL_PRIORITISATION_DATE -> clinical_prioritisation_date\n  PRIORITY_TYPE -> priority_type\n  LATEST_TREATMENT_FUNCTION_CODE -> latest_treatment_function_code\n  MAIN_SPECIALTY_CODE -> main_specialty_code\n  INTENDED_OPCS_CODE -> intended_opcs_code\n  INTENDED_ICD10_CODE -> intended_icd10_code\n  NETWORK -> network\n  LOCAL_SUB_SPECIALTY_CODE -> local_sub_specialty_code\n  FUTURE_BOOKED_OP_DATE -> future_booked_op_date\n  ROW_ID -> row_id"
    )
}}
select
    "CENSUS_DATE" as census_date,
    "PROVIDER_CODE" as provider_code,
    "PROVIDER_SITE" as provider_site,
    "COUNT" as count,
    "CCG_CODE" as ccg_code,
    "GP_PRACTICE_CODE" as gp_practice_code,
    "GENDER" as gender,
    "ETHNICITY" as ethnicity,
    "AGE" as age,
    "REFERRAL_RECEIVED_DATE" as referral_received_date,
    "REFERRAL_SOURCE" as referral_source,
    "REFERRING_ORG_CODE" as referring_org_code,
    "IS_EREFERRAL" as is_ereferral,
    "ASI_FLAG" as asi_flag,
    "RTT_WAIT_DAYS" as rtt_wait_days,
    "ADMITTED_NONADMITTED_LIST" as admitted_nonadmitted_list,
    "HAS_HAD_FIRST_OP" as has_had_first_op,
    "PATIENT_REFUSAL_DEFERRAL_DATE" as patient_refusal_deferral_date,
    "TCI_STATUS" as tci_status,
    "TCI_DECISION_DATE" as tci_decision_date,
    "TCI_DATE" as tci_date,
    "CLINICAL_PRIORITY" as clinical_priority,
    "CLINICAL_PRIORITISATION_DATE" as clinical_prioritisation_date,
    "PRIORITY_TYPE" as priority_type,
    "LATEST_TREATMENT_FUNCTION_CODE" as latest_treatment_function_code,
    "MAIN_SPECIALTY_CODE" as main_specialty_code,
    "INTENDED_OPCS_CODE" as intended_opcs_code,
    "INTENDED_ICD10_CODE" as intended_icd10_code,
    "NETWORK" as network,
    "LOCAL_SUB_SPECIALTY_CODE" as local_sub_specialty_code,
    "FUTURE_BOOKED_OP_DATE" as future_booked_op_date,
    "ROW_ID" as row_id
from {{ source('ptl', 'PTL_WEEKLY_UPLOAD_WITH_ROW_IDS') }}
