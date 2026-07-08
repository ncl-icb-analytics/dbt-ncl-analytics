{{
    config(
        description="Raw layer (SUS Monthly admitted patient care episodes and procedures). 1:1 passthrough with cleaned column names. \nSource: DATA_LAKE.SUS_AE.EncounterInvestigation \ndbt: source(''sus_apc_monthly'', ''EncounterInvestigation'') \nColumns:\n  SK_EncounterID -> sk_encounter_id\n  SK_EncounterID -> sk_encounter_id_1\n  InvestigationNumber -> investigation_number\n  InvestigationNumber -> investigation_number_1\n  InvestigationCode -> investigation_code\n  InvestigationCode -> investigation_code_1\n  ActivityPeriod -> activity_period\n  ActivityPeriod -> activity_period_1"
    )
}}
select
    "SK_EncounterID" as sk_encounter_id,
    "SK_EncounterID" as sk_encounter_id_1,
    "InvestigationNumber" as investigation_number,
    "InvestigationNumber" as investigation_number_1,
    "InvestigationCode" as investigation_code,
    "InvestigationCode" as investigation_code_1,
    "ActivityPeriod" as activity_period,
    "ActivityPeriod" as activity_period_1
from {{ source('sus_apc_monthly', 'EncounterInvestigation') }}
