{{
    config(
        description="Raw layer (AIC pipelines). 1:1 passthrough with cleaned column names. \nSource: DATA_LAKE__NCL.AIC_DEV.BASE_NHSBSA__APID_VPID \ndbt: source(''aic'', ''BASE_NHSBSA__APID_VPID'') \nColumns:\n  APID -> apid\n  VPID -> vpid"
    )
}}
select
    "APID" as apid,
    "VPID" as vpid
from {{ source('aic', 'BASE_NHSBSA__APID_VPID') }}
