-- Staging model for wl.WL_ClockStarts_Data
-- Source: "DATA_LAKE"."WL"
{% if source.get('description') %}
-- Description: Waiting lists and patient pathway data
{% endif %}

select
    "DATE_AND_TIME_DATA_SET_CREATED" as date_and_time_data_set_created,
    "Week_Ending_Date" as week_ending_date,
    "PATIENT_PATHWAY_IDENTIFIER" as patient_pathway_identifier,
    "ORGANISATION_CODE_PATIENT_PATHWAY_IDENTIFIER_ISSUER" as organisation_code_patient_pathway_identifier_issuer,
    "REFERRAL_TO_TREATMENT_PERIOD_START_DATE" as referral_to_treatment_period_start_date,
    "ORGANISATION_IDENTIFIER_CODE_OF_PROVIDER" as organisation_identifier_code_of_provider,
    "ORGANISATION_IDENTIFIER_CODE_OF_COMMISSIONER" as organisation_identifier_code_of_commissioner,
    "ACTIVITY_TREATMENT_FUNCTION_CODE" as activity_treatment_function_code,
    "derSubmissionId" as dersubmissionid,
    "derRowId" as derrowid,
    "dmIcbCommissioner" as dmicbcommissioner,
    "dmSubIcbCommissioner" as dmsubicbcommissioner,
    "Pseudo NHS_NUMBER" as pseudo_nhs_number,
    "LOCAL_PATIENT_IDENTIFIER" as local_patient_identifier,
    "PRIORITY_TYPE_CODE" as priority_type_code,
    "SOURCE_OF_REFERRAL" as source_of_referral,
    "PERSON_STATED_GENDER_CODE" as person_stated_gender_code,
    "ETHNIC_CATEGORY" as ethnic_category,
    "REFERRAL_TO_TREATMENT_PERIOD_STATUS" as referral_to_treatment_period_status,
    "der_Age_WeekEndingDate" as der_age_weekendingdate,
    "der_Age_at_Referral_To_Treatment_Period_Start_Date" as der_age_at_referral_to_treatment_period_start_date,
    "der_AgeBand_WeekEndingDate" as der_ageband_weekendingdate,
    "der_AgeBand_at_Referral_To_Treatment_Period_Start_Date" as der_ageband_at_referral_to_treatment_period_start_date,
    "derCCGofPractice" as derccgofpractice,
    "derCCGofResidence" as derccgofresidence,
    "derPracticeCode" as derpracticecode,
    "derLSOA" as derlsoa,
    "derLSOA2021" as derlsoa2021
from {{ source('wl', 'WL_ClockStarts_Data') }}
