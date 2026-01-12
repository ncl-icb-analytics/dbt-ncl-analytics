{{
    config(
        description="Raw layer (Analyst-managed reference datasets and business rules). 1:1 passthrough with cleaned column names. \nSource: DATA_LAKE__NCL.ANALYST_MANAGED.BP_THRESHOLDS \ndbt: source(''reference_analyst_managed'', ''BP_THRESHOLDS'') \nColumns:\n  THRESHOLD_RULE_ID -> threshold_rule_id\n  PROGRAMME_OR_GUIDELINE -> programme_or_guideline\n  DESCRIPTION -> description\n  PATIENT_GROUP -> patient_group\n  THRESHOLD_TYPE -> threshold_type\n  SYSTOLIC_THRESHOLD -> systolic_threshold\n  DIASTOLIC_THRESHOLD -> diastolic_threshold\n  OPERATOR -> operator\n  NOTES -> notes"
    )
}}
select
    "THRESHOLD_RULE_ID" as threshold_rule_id,
    "PROGRAMME_OR_GUIDELINE" as programme_or_guideline,
    "DESCRIPTION" as description,
    "PATIENT_GROUP" as patient_group,
    "THRESHOLD_TYPE" as threshold_type,
    "SYSTOLIC_THRESHOLD" as systolic_threshold,
    "DIASTOLIC_THRESHOLD" as diastolic_threshold,
    "OPERATOR" as operator,
    "NOTES" as notes
from {{ source('reference_analyst_managed', 'BP_THRESHOLDS') }}
