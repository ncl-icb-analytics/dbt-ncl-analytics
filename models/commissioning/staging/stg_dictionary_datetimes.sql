-- Staging model for dictionary.DateTimes
-- Source: "Dictionary"."dbo"
{% if source.get('description') %}
-- Description: Reference data including PDS and lookup tables
{% endif %}

select
    "SK_DateTime" as sk_datetime,
    "SK_Date" as sk_date,
    "SK_Time" as sk_time,
    "FullDateTime" as fulldatetime,
    "FullDate" as fulldate,
    "FullTime" as fulltime
from {{ source('dictionary', 'DateTimes') }}
