-- Staging model for dictionary_dbo.DateTimes
-- Source: "Dictionary"."dbo"
-- Description: Reference data including PDS and lookup tables

select
    "SK_DateTime" as sk_datetime,
    "SK_Date" as sk_date,
    "SK_Time" as sk_time,
    "FullDateTime" as fulldatetime,
    "FullDate" as fulldate,
    "FullTime" as fulltime
from {{ source('dictionary_dbo', 'DateTimes') }}
