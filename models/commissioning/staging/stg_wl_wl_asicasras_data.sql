-- Staging model for wl.WL_AsiCasRas_Data
-- Source: "DATA_LAKE"."WL"
-- Description: Waiting lists and patient pathway data

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
