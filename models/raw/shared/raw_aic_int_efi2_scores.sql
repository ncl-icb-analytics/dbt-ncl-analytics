{{
    config(
        description="Raw layer (AIC pipelines). 1:1 passthrough with cleaned column names. \nSource: DATA_LAKE__NCL.AIC_DEV.INT_EFI2_SCORES \ndbt: source(''aic'', ''INT_EFI2_SCORES'') \nColumns:\n  PERSON_ID -> person_id\n  END_DATE -> end_date\n  EFI_SCORE -> efi_score\n  CATEGORY -> category\n  AGE_AT_END -> age_at_end\n  GENDER -> gender\n  DATE_OF_DEATH -> date_of_death"
    )
}}
select
    "PERSON_ID" as person_id,
    "END_DATE" as end_date,
    "EFI_SCORE" as efi_score,
    "CATEGORY" as category,
    "AGE_AT_END" as age_at_end,
    "GENDER" as gender,
    "DATE_OF_DEATH" as date_of_death
from {{ source('aic', 'INT_EFI2_SCORES') }}
