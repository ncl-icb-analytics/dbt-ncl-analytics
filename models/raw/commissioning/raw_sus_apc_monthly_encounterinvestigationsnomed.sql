{{
    config(
        description="Raw layer (SUS Monthly admitted patient care episodes and procedures). 1:1 passthrough with cleaned column names. \nSource: DATA_LAKE.SUS_AE.EncounterInvestigationSNOMED \ndbt: source(''sus_apc_monthly'', ''EncounterInvestigationSNOMED'') \nColumns:\n  SK_EncounterID -> sk_encounter_id\n  SK_EncounterID -> sk_encounter_id_1\n  Sequence_Number -> sequence_number\n  Sequence_Number -> sequence_number_1\n  Code -> code\n  Code -> code_1\n  Date -> date\n  Date -> date_1\n  Time -> time\n  Time -> time_1\n  Is_Approved -> is_approved\n  Is_Approved -> is_approved_1"
    )
}}
select
    "SK_EncounterID" as sk_encounter_id,
    "SK_EncounterID" as sk_encounter_id_1,
    "Sequence_Number" as sequence_number,
    "Sequence_Number" as sequence_number_1,
    "Code" as code,
    "Code" as code_1,
    "Date" as date,
    "Date" as date_1,
    "Time" as time,
    "Time" as time_1,
    "Is_Approved" as is_approved,
    "Is_Approved" as is_approved_1
from {{ source('sus_apc_monthly', 'EncounterInvestigationSNOMED') }}
