{{
    config(
        description="Raw layer (SUS Monthly admitted patient care episodes and procedures). 1:1 passthrough with cleaned column names. \nSource: DATA_LAKE.SUS_AE.Encounter \ndbt: source(''sus_apc_monthly'', ''Encounter'') \nColumns:\n  SK_EncounterID -> sk_encounter_id\n  SK_EncounterID -> sk_encounter_id_1\n  RowID -> row_id\n  RowID -> row_id_1\n  SK_SUSDataMartID -> sk_sus_data_mart_id\n  SK_SUSDataMartID -> sk_sus_data_mart_id_1\n  ActivityPeriod -> activity_period\n  ActivityPeriod -> activity_period_1"
    )
}}
select
    "SK_EncounterID" as sk_encounter_id,
    "SK_EncounterID" as sk_encounter_id_1,
    "RowID" as row_id,
    "RowID" as row_id_1,
    "SK_SUSDataMartID" as sk_sus_data_mart_id,
    "SK_SUSDataMartID" as sk_sus_data_mart_id_1,
    "ActivityPeriod" as activity_period,
    "ActivityPeriod" as activity_period_1
from {{ source('sus_apc_monthly', 'Encounter') }}
