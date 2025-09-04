-- Staging model for wl.WL_PIFU_Data
-- Source: "DATA_LAKE"."WL"
-- Description: Waiting lists and patient pathway data

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
