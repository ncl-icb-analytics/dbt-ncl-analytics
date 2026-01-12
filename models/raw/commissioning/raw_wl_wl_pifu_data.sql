{{
    config(
        description="Raw layer (Waiting lists and patient pathway data). 1:1 passthrough with cleaned column names. \nSource: DATA_LAKE.WL.WL_PIFU_Data \ndbt: source(''wl'', ''WL_PIFU_Data'') \nColumns:\n  DATE_AND_TIME_DATA_SET_CREATED -> date_and_time_data_set_created\n  Week_Ending_Date -> week_ending_date\n  Pseudo NHS_NUMBER -> pseudo_nhs_number\n  PATIENT_PATHWAY_IDENTIFIER -> patient_pathway_identifier\n  ORGANISATION_CODE_PATIENT_PATHWAY_IDENTIFIER_ISSUER -> organisation_code_patient_pathway_identifier_issuer\n  ORGANISATION_IDENTIFIER_CODE_OF_PROVIDER -> organisation_identifier_code_of_provider\n  ORGANISATION_SITE_IDENTIFIER_OF_TREATMENT -> organisation_site_identifier_of_treatment\n  ORGANISATION_IDENTIFIER_CODE_OF_COMMISSIONER -> organisation_identifier_code_of_commissioner\n  ACTIVITY_TREATMENT_FUNCTION_CODE -> activity_treatment_function_code\n  Waiting_List_Type -> waiting_list_type\n  Initiation_Date -> initiation_date\n  Expiry_Date -> expiry_date\n  der_Age_WeekEndingDate -> der_age_week_ending_date\n  der_AgeBand_WeekEndingDate -> der_age_band_week_ending_date\n  derSubmissionId -> der_submission_id\n  derRowId -> der_row_id\n  derCCGofPractice -> der_ccg_of_practice\n  derCCGofResidence -> der_ccg_of_residence\n  derPracticeCode -> der_practice_code\n  derLSOA -> der_lsoa\n  derLSOA2021 -> der_lsoa2021"
    )
}}
select
    "DATE_AND_TIME_DATA_SET_CREATED" as date_and_time_data_set_created,
    "Week_Ending_Date" as week_ending_date,
    "Pseudo NHS_NUMBER" as pseudo_nhs_number,
    "PATIENT_PATHWAY_IDENTIFIER" as patient_pathway_identifier,
    "ORGANISATION_CODE_PATIENT_PATHWAY_IDENTIFIER_ISSUER" as organisation_code_patient_pathway_identifier_issuer,
    "ORGANISATION_IDENTIFIER_CODE_OF_PROVIDER" as organisation_identifier_code_of_provider,
    "ORGANISATION_SITE_IDENTIFIER_OF_TREATMENT" as organisation_site_identifier_of_treatment,
    "ORGANISATION_IDENTIFIER_CODE_OF_COMMISSIONER" as organisation_identifier_code_of_commissioner,
    "ACTIVITY_TREATMENT_FUNCTION_CODE" as activity_treatment_function_code,
    "Waiting_List_Type" as waiting_list_type,
    "Initiation_Date" as initiation_date,
    "Expiry_Date" as expiry_date,
    "der_Age_WeekEndingDate" as der_age_week_ending_date,
    "der_AgeBand_WeekEndingDate" as der_age_band_week_ending_date,
    "derSubmissionId" as der_submission_id,
    "derRowId" as der_row_id,
    "derCCGofPractice" as der_ccg_of_practice,
    "derCCGofResidence" as der_ccg_of_residence,
    "derPracticeCode" as der_practice_code,
    "derLSOA" as der_lsoa,
    "derLSOA2021" as der_lsoa2021
from {{ source('wl', 'WL_PIFU_Data') }}
