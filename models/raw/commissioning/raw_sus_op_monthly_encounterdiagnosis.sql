{{
    config(
        description="Raw layer (SUS Monthly outpatient appointments and activity). 1:1 passthrough with cleaned column names. \nSource: DATA_LAKE.SUS_OP.EncounterDiagnosis \ndbt: source(''sus_op_monthly'', ''EncounterDiagnosis'') \nColumns:\n  SK_EncounterID -> sk_encounter_id\n  DiagnosisNumber -> diagnosis_number\n  DiagnosisCode -> diagnosis_code\n  ActivityPeriod -> activity_period"
    )
}}
select
    "SK_EncounterID" as sk_encounter_id,
    "DiagnosisNumber" as diagnosis_number,
    "DiagnosisCode" as diagnosis_code,
    "ActivityPeriod" as activity_period
from {{ source('sus_op_monthly', 'EncounterDiagnosis') }}
