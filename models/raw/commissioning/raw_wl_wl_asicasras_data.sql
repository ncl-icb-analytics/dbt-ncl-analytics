{{
    config(
        description="Raw layer (Waiting lists and patient pathway data). 1:1 passthrough with cleaned column names. \nSource: DATA_LAKE.WL.WL_AsiCasRas_Data \ndbt: source(''wl'', ''WL_AsiCasRas_Data'') \nColumns:\n  DATE_AND_TIME_DATA_SET_CREATED -> date_and_time_data_set_created\n  Week_Ending_Date -> week_ending_date\n  REFERRAL_REQUEST_RECEIVED_DATE -> referral_request_received_date\n  Referral_Identifier -> referral_identifier\n  ORGANISATION_IDENTIFIER_CODE_OF_PROVIDER -> organisation_identifier_code_of_provider\n  ACTIVITY_TREATMENT_FUNCTION_CODE -> activity_treatment_function_code\n  derSubmissionId -> der_submission_id\n  derRowId -> der_row_id\n  Pseudo NHS_NUMBER -> pseudo_nhs_number\n  ORGANISATION_IDENTIFIER_CODE_OF_COMMISSIONER -> organisation_identifier_code_of_commissioner\n  Last_PAS_Validation_Date -> last_pas_validation_date\n  derCCGofPractice -> der_ccg_of_practice\n  derCCGofResidence -> der_ccg_of_residence\n  derPracticeCode -> der_practice_code\n  derLSOA -> der_lsoa\n  derLSOA2021 -> der_lsoa2021"
    )
}}
select
    "DATE_AND_TIME_DATA_SET_CREATED" as date_and_time_data_set_created,
    "Week_Ending_Date" as week_ending_date,
    "REFERRAL_REQUEST_RECEIVED_DATE" as referral_request_received_date,
    "Referral_Identifier" as referral_identifier,
    "ORGANISATION_IDENTIFIER_CODE_OF_PROVIDER" as organisation_identifier_code_of_provider,
    "ACTIVITY_TREATMENT_FUNCTION_CODE" as activity_treatment_function_code,
    "derSubmissionId" as der_submission_id,
    "derRowId" as der_row_id,
    "Pseudo NHS_NUMBER" as pseudo_nhs_number,
    "ORGANISATION_IDENTIFIER_CODE_OF_COMMISSIONER" as organisation_identifier_code_of_commissioner,
    "Last_PAS_Validation_Date" as last_pas_validation_date,
    "derCCGofPractice" as der_ccg_of_practice,
    "derCCGofResidence" as der_ccg_of_residence,
    "derPracticeCode" as der_practice_code,
    "derLSOA" as der_lsoa,
    "derLSOA2021" as der_lsoa2021
from {{ source('wl', 'WL_AsiCasRas_Data') }}
