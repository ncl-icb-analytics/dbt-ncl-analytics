{{
    config(
        description="Raw layer (SUS Monthly admitted patient care episodes and procedures). 1:1 passthrough with cleaned column names. \nSource: DATA_LAKE.SUS_AE.EncounterLegalStatus \ndbt: source(''sus_apc_monthly'', ''EncounterLegalStatus'') \nColumns:\n  SK_EncounterID -> sk_encounter_id\n  SK_EncounterID -> sk_encounter_id_1\n  Sequence_Number -> sequence_number\n  Sequence_Number -> sequence_number_1\n  SK_MentalHealthLegalStatus_ID -> sk_mental_health_legal_status_id\n  SK_MentalHealthLegalStatus_ID -> sk_mental_health_legal_status_id_1\n  Assignment_Period_Start_Date -> assignment_period_start_date\n  Assignment_Period_Start_Date -> assignment_period_start_date_1\n  Assignment_Period_Start_Time -> assignment_period_start_time\n  Assignment_Period_Start_Time -> assignment_period_start_time_1\n  Expiry_Date -> expiry_date\n  Expiry_Date -> expiry_date_1\n  Expiry_Time -> expiry_time\n  Expiry_Time -> expiry_time_1\n  Is_Approved -> is_approved\n  Is_Approved -> is_approved_1"
    )
}}
select
    "SK_EncounterID" as sk_encounter_id,
    "SK_EncounterID" as sk_encounter_id_1,
    "Sequence_Number" as sequence_number,
    "Sequence_Number" as sequence_number_1,
    "SK_MentalHealthLegalStatus_ID" as sk_mental_health_legal_status_id,
    "SK_MentalHealthLegalStatus_ID" as sk_mental_health_legal_status_id_1,
    "Assignment_Period_Start_Date" as assignment_period_start_date,
    "Assignment_Period_Start_Date" as assignment_period_start_date_1,
    "Assignment_Period_Start_Time" as assignment_period_start_time,
    "Assignment_Period_Start_Time" as assignment_period_start_time_1,
    "Expiry_Date" as expiry_date,
    "Expiry_Date" as expiry_date_1,
    "Expiry_Time" as expiry_time,
    "Expiry_Time" as expiry_time_1,
    "Is_Approved" as is_approved,
    "Is_Approved" as is_approved_1
from {{ source('sus_apc_monthly', 'EncounterLegalStatus') }}
