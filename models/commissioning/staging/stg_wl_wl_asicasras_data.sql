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
    "derSubmissionId" as dersubmissionid,
    "derRowId" as derrowid,
    "Pseudo NHS_NUMBER" as pseudo_nhs_number,
    "ORGANISATION_IDENTIFIER_CODE_OF_COMMISSIONER" as organisation_identifier_code_of_commissioner,
    "Last_PAS_Validation_Date" as last_pas_validation_date,
    "derCCGofPractice" as derccgofpractice,
    "derCCGofResidence" as derccgofresidence,
    "derPracticeCode" as derpracticecode,
    "derLSOA" as derlsoa,
    "derLSOA2021" as derlsoa2021
from {{ source('wl', 'WL_AsiCasRas_Data') }}
