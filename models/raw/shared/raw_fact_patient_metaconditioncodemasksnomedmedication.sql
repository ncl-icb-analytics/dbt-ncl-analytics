{{
    config(
        description="Raw layer (Patient fact tables). 1:1 passthrough with cleaned column names. \nSource: DATA_LAKE.FACT_PATIENT.MetaConditionCodeMaskSnomedMedication \ndbt: source(''fact_patient'', ''MetaConditionCodeMaskSnomedMedication'') \nColumns:\n  SK_ConditionTypeID -> sk_condition_type_id\n  Snomed -> snomed\n  IsExclude -> is_exclude"
    )
}}
select
    "SK_ConditionTypeID" as sk_condition_type_id,
    "Snomed" as snomed,
    "IsExclude" as is_exclude
from {{ source('fact_patient', 'MetaConditionCodeMaskSnomedMedication') }}
