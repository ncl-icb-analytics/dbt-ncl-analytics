{{
    config(
        description="Raw layer (SUS Monthly admitted patient care episodes and procedures). 1:1 passthrough with cleaned column names. \nSource: DATA_LAKE.SUS_AE.EncounterTreatment \ndbt: source(''sus_apc_monthly'', ''EncounterTreatment'') \nColumns:\n  SK_EncounterID -> sk_encounter_id\n  SK_EncounterID -> sk_encounter_id_1\n  TreatmentNumber -> treatment_number\n  TreatmentNumber -> treatment_number_1\n  TreatmentCode -> treatment_code\n  TreatmentCode -> treatment_code_1\n  ProcedureDate -> procedure_date\n  ProcedureDate -> procedure_date_1\n  ActivityPeriod -> activity_period\n  ActivityPeriod -> activity_period_1"
    )
}}
select
    "SK_EncounterID" as sk_encounter_id,
    "SK_EncounterID" as sk_encounter_id_1,
    "TreatmentNumber" as treatment_number,
    "TreatmentNumber" as treatment_number_1,
    "TreatmentCode" as treatment_code,
    "TreatmentCode" as treatment_code_1,
    "ProcedureDate" as procedure_date,
    "ProcedureDate" as procedure_date_1,
    "ActivityPeriod" as activity_period,
    "ActivityPeriod" as activity_period_1
from {{ source('sus_apc_monthly', 'EncounterTreatment') }}
