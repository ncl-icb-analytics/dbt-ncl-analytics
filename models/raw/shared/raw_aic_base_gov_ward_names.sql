{{
    config(
        description="Raw layer (AIC pipelines). 1:1 passthrough with cleaned column names. \nSource: DATA_LAKE__NCL.AIC_DEV.BASE_GOV__WARD_NAMES \ndbt: source(''aic'', ''BASE_GOV__WARD_NAMES'') \nColumns:\n  WARD_CODE -> ward_code\n  WARD_NAME -> ward_name"
    )
}}
select
    "WARD_CODE" as ward_code,
    "WARD_NAME" as ward_name
from {{ source('aic', 'BASE_GOV__WARD_NAMES') }}
