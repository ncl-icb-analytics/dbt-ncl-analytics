{{
    config(
        description="Raw layer (Reference data including PDS and lookup tables). 1:1 passthrough with cleaned column names. \nSource: Dictionary.dbo.DateTimes \ndbt: source(''dictionary_dbo'', ''DateTimes'') \nColumns:\n  SK_DateTime -> sk_date_time\n  SK_Date -> sk_date\n  SK_Time -> sk_time\n  FullDateTime -> full_date_time\n  FullDate -> full_date\n  FullTime -> full_time"
    )
}}
select
    "SK_DateTime" as sk_date_time,
    "SK_Date" as sk_date,
    "SK_Time" as sk_time,
    "FullDateTime" as full_date_time,
    "FullDate" as full_date,
    "FullTime" as full_time
from {{ source('dictionary_dbo', 'DateTimes') }}
