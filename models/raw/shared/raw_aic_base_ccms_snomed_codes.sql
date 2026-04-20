{{
    config(
        description="Raw layer (AIC pipelines). 1:1 passthrough with cleaned column names. \nSource: DATA_LAKE__NCL.AIC_DEV.BASE_CCMS_SNOMED_CODES \ndbt: source(''aic'', ''BASE_CCMS_SNOMED_CODES'') \nColumns:\n  CONDITIONID -> conditionid\n  CONDITIONNAME -> conditionname\n  CONCEPTID -> conceptid\n  PRIMARYTERM -> primaryterm"
    )
}}
select
    "CONDITIONID" as conditionid,
    "CONDITIONNAME" as conditionname,
    "CONCEPTID" as conceptid,
    "PRIMARYTERM" as primaryterm
from {{ source('aic', 'BASE_CCMS_SNOMED_CODES') }}
