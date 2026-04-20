{{
    config(
        description="Raw layer (AIC pipelines). 1:1 passthrough with cleaned column names. \nSource: DATA_LAKE__NCL.AIC_DEV.BASE_CCMS_DMD_CODES \ndbt: source(''aic'', ''BASE_CCMS_DMD_CODES'') \nColumns:\n  CONDITIONID -> conditionid\n  CONDITIONNAME -> conditionname\n  PRODUCTID -> productid\n  PRIMARYTERM -> primaryterm"
    )
}}
select
    "CONDITIONID" as conditionid,
    "CONDITIONNAME" as conditionname,
    "PRODUCTID" as productid,
    "PRIMARYTERM" as primaryterm
from {{ source('aic', 'BASE_CCMS_DMD_CODES') }}
