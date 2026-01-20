{{
    config(
        description="Raw layer (Reference data including PDS and lookup tables). 1:1 passthrough with cleaned column names. \nSource: Dictionary.dbo.CarerSupportIndicator \ndbt: source(''dictionary_dbo'', ''CarerSupportIndicator'') \nColumns:\n  SK_CarerSupportIndicatorID -> sk_carer_support_indicator_id\n  BK_CarerSupportIndicator -> bk_carer_support_indicator\n  CarerSupportIndicator -> carer_support_indicator"
    )
}}
select
    "SK_CarerSupportIndicatorID" as sk_carer_support_indicator_id,
    "BK_CarerSupportIndicator" as bk_carer_support_indicator,
    "CarerSupportIndicator" as carer_support_indicator
from {{ source('dictionary_dbo', 'CarerSupportIndicator') }}
