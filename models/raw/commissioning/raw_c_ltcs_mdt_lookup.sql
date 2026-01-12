{{
    config(
        description="Raw layer (C-LTCS tables). 1:1 passthrough with cleaned column names. \nSource: DEV__PUBLISHED_REPORTING__DIRECT_CARE.C_LTCS.MDT_LOOKUP \ndbt: source(''c_ltcs'', ''MDT_LOOKUP'') \nColumns:\n  PCN_CODE -> pcn_code\n  MDT_DATE -> mdt_date"
    )
}}
select
    "PCN_CODE" as pcn_code,
    "MDT_DATE" as mdt_date
from {{ source('c_ltcs', 'MDT_LOOKUP') }}
