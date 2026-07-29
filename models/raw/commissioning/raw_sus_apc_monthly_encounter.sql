{{
    config(
        description="Raw layer (SUS Monthly admitted patient care episodes and procedures). 1:1 passthrough with cleaned column names. \nSource: DATA_LAKE.SUS_IP.Encounter \ndbt: source(''sus_apc_monthly'', ''Encounter'') \nColumns:\n  SK_EncounterID -> sk_encounter_id\n  RowID -> row_id"
    )
}}
select
    "SK_EncounterID" as sk_encounter_id,
    "RowID" as row_id
from {{ source('sus_apc_monthly', 'Encounter') }}
