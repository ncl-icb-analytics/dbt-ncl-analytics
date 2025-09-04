-- Staging model for dictionary_dbo.DateTimes
-- Source: "Dictionary"."dbo"
-- Description: Reference data including PDS and lookup tables

select
    "SK_DateTime" as sk_date_time,
    "SK_Date" as sk_date,
    "SK_Time" as sk_time,
    "FullDateTime" as full_date_time,
    "FullDate" as full_date,
    "FullTime" as full_time
from {{ source('dictionary_dbo', 'DateTimes') }}
