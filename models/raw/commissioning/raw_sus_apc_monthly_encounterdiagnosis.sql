{{
    config(
        description="Raw layer (SUS Monthly admitted patient care episodes and procedures). 1:1 passthrough with cleaned column names. \nSource: DATA_LAKE.SUS_AE.EncounterDiagnosis \ndbt: source(''sus_apc_monthly'', ''EncounterDiagnosis'') \nColumns:\n  SK_EncounterID -> sk_encounter_id\n  SK_EncounterID -> sk_encounter_id_1\n  DiagnosisNumber -> diagnosis_number\n  DiagnosisNumber -> diagnosis_number_1\n  DiagnosisCode -> diagnosis_code\n  DiagnosisCode -> diagnosis_code_1\n  ActivityPeriod -> activity_period\n  ActivityPeriod -> activity_period_1"
    )
}}
select
    "SK_EncounterID" as sk_encounter_id,
    "SK_EncounterID" as sk_encounter_id_1,
    "DiagnosisNumber" as diagnosis_number,
    "DiagnosisNumber" as diagnosis_number_1,
    "DiagnosisCode" as diagnosis_code,
    "DiagnosisCode" as diagnosis_code_1,
    "ActivityPeriod" as activity_period,
    "ActivityPeriod" as activity_period_1
from {{ source('sus_apc_monthly', 'EncounterDiagnosis') }}
