{{
    config(
        description="Raw layer (SUS Monthly admitted patient care episodes and procedures). 1:1 passthrough with cleaned column names. \nSource: DATA_LAKE.SUS_IP.EncounterProcedure \ndbt: source(''sus_apc_monthly'', ''EncounterProcedure'') \nColumns:\n  SK_EncounterID -> sk_encounter_id\n  ProcedureNumber -> procedure_number\n  ProcedureCode -> procedure_code\n  ProcedureDate -> procedure_date\n  ActivityPeriod -> activity_period"
    )
}}
select
    "SK_EncounterID" as sk_encounter_id,
    "ProcedureNumber" as procedure_number,
    "ProcedureCode" as procedure_code,
    "ProcedureDate" as procedure_date,
    "ActivityPeriod" as activity_period
from {{ source('sus_apc_monthly', 'EncounterProcedure') }}
