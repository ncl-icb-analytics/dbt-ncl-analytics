{{
    config(
        description="Raw layer (Waiting lists and patient pathway data). 1:1 passthrough with cleaned column names. \nSource: DATA_LAKE.WL.WL_ClockStarts_Data \ndbt: source(''wl'', ''WL_ClockStarts_Data'') \nColumns:\n  DATE_AND_TIME_DATA_SET_CREATED -> date_and_time_data_set_created\n  Week_Ending_Date -> week_ending_date\n  PATIENT_PATHWAY_IDENTIFIER -> patient_pathway_identifier\n  ORGANISATION_CODE_PATIENT_PATHWAY_IDENTIFIER_ISSUER -> organisation_code_patient_pathway_identifier_issuer\n  REFERRAL_TO_TREATMENT_PERIOD_START_DATE -> referral_to_treatment_period_start_date\n  ORGANISATION_IDENTIFIER_CODE_OF_PROVIDER -> organisation_identifier_code_of_provider\n  ORGANISATION_IDENTIFIER_CODE_OF_COMMISSIONER -> organisation_identifier_code_of_commissioner\n  ACTIVITY_TREATMENT_FUNCTION_CODE -> activity_treatment_function_code\n  derSubmissionId -> der_submission_id\n  derRowId -> der_row_id\n  dmIcbCommissioner -> dm_icb_commissioner\n  dmSubIcbCommissioner -> dm_sub_icb_commissioner\n  Pseudo NHS_NUMBER -> pseudo_nhs_number\n  LOCAL_PATIENT_IDENTIFIER -> local_patient_identifier\n  PRIORITY_TYPE_CODE -> priority_type_code\n  SOURCE_OF_REFERRAL -> source_of_referral\n  PERSON_STATED_GENDER_CODE -> person_stated_gender_code\n  ETHNIC_CATEGORY -> ethnic_category\n  REFERRAL_TO_TREATMENT_PERIOD_STATUS -> referral_to_treatment_period_status\n  der_Age_WeekEndingDate -> der_age_week_ending_date\n  der_Age_at_Referral_To_Treatment_Period_Start_Date -> der_age_at_referral_to_treatment_period_start_date\n  der_AgeBand_WeekEndingDate -> der_age_band_week_ending_date\n  der_AgeBand_at_Referral_To_Treatment_Period_Start_Date -> der_age_band_at_referral_to_treatment_period_start_date\n  derCCGofPractice -> der_ccg_of_practice\n  derCCGofResidence -> der_ccg_of_residence\n  derPracticeCode -> der_practice_code\n  derLSOA -> der_lsoa\n  derLSOA2021 -> der_lsoa2021\n  UNIQUE_BOOKING_REFERENCE_NUMBER -> unique_booking_reference_number"
    )
}}
select
    "DATE_AND_TIME_DATA_SET_CREATED" as date_and_time_data_set_created,
    "Week_Ending_Date" as week_ending_date,
    "PATIENT_PATHWAY_IDENTIFIER" as patient_pathway_identifier,
    "ORGANISATION_CODE_PATIENT_PATHWAY_IDENTIFIER_ISSUER" as organisation_code_patient_pathway_identifier_issuer,
    "REFERRAL_TO_TREATMENT_PERIOD_START_DATE" as referral_to_treatment_period_start_date,
    "ORGANISATION_IDENTIFIER_CODE_OF_PROVIDER" as organisation_identifier_code_of_provider,
    "ORGANISATION_IDENTIFIER_CODE_OF_COMMISSIONER" as organisation_identifier_code_of_commissioner,
    "ACTIVITY_TREATMENT_FUNCTION_CODE" as activity_treatment_function_code,
    "derSubmissionId" as der_submission_id,
    "derRowId" as der_row_id,
    "dmIcbCommissioner" as dm_icb_commissioner,
    "dmSubIcbCommissioner" as dm_sub_icb_commissioner,
    "Pseudo NHS_NUMBER" as pseudo_nhs_number,
    "LOCAL_PATIENT_IDENTIFIER" as local_patient_identifier,
    "PRIORITY_TYPE_CODE" as priority_type_code,
    "SOURCE_OF_REFERRAL" as source_of_referral,
    "PERSON_STATED_GENDER_CODE" as person_stated_gender_code,
    "ETHNIC_CATEGORY" as ethnic_category,
    "REFERRAL_TO_TREATMENT_PERIOD_STATUS" as referral_to_treatment_period_status,
    "der_Age_WeekEndingDate" as der_age_week_ending_date,
    "der_Age_at_Referral_To_Treatment_Period_Start_Date" as der_age_at_referral_to_treatment_period_start_date,
    "der_AgeBand_WeekEndingDate" as der_age_band_week_ending_date,
    "der_AgeBand_at_Referral_To_Treatment_Period_Start_Date" as der_age_band_at_referral_to_treatment_period_start_date,
    "derCCGofPractice" as der_ccg_of_practice,
    "derCCGofResidence" as der_ccg_of_residence,
    "derPracticeCode" as der_practice_code,
    "derLSOA" as der_lsoa,
    "derLSOA2021" as der_lsoa2021,
    "UNIQUE_BOOKING_REFERENCE_NUMBER" as unique_booking_reference_number
from {{ source('wl', 'WL_ClockStarts_Data') }}
