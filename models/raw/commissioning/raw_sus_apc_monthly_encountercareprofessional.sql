{{
    config(
        description="Raw layer (SUS Monthly admitted patient care episodes and procedures). 1:1 passthrough with cleaned column names. \nSource: DATA_LAKE.SUS_AE.EncounterCareProfessional \ndbt: source(''sus_apc_monthly'', ''EncounterCareProfessional'') \nColumns:\n  SK_EncounterID -> sk_encounter_id\n  SK_EncounterID -> sk_encounter_id_1\n  Sequence_Number -> sequence_number\n  Sequence_Number -> sequence_number_1\n  Code -> code\n  Code -> code_1\n  SK_Issuer_Code_ID -> sk_issuer_code_id\n  SK_Issuer_Code_ID -> sk_issuer_code_id_1\n  Tier -> tier\n  Tier -> tier_1\n  Discharge_Responsibility_Indicator -> discharge_responsibility_indicator\n  Discharge_Responsibility_Indicator -> discharge_responsibility_indicator_1\n  Is_Approved -> is_approved\n  Is_Approved -> is_approved_1"
    )
}}
select
    "SK_EncounterID" as sk_encounter_id,
    "SK_EncounterID" as sk_encounter_id_1,
    "Sequence_Number" as sequence_number,
    "Sequence_Number" as sequence_number_1,
    "Code" as code,
    "Code" as code_1,
    "SK_Issuer_Code_ID" as sk_issuer_code_id,
    "SK_Issuer_Code_ID" as sk_issuer_code_id_1,
    "Tier" as tier,
    "Tier" as tier_1,
    "Discharge_Responsibility_Indicator" as discharge_responsibility_indicator,
    "Discharge_Responsibility_Indicator" as discharge_responsibility_indicator_1,
    "Is_Approved" as is_approved,
    "Is_Approved" as is_approved_1
from {{ source('sus_apc_monthly', 'EncounterCareProfessional') }}
