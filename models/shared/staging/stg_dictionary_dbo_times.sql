-- Staging model for dictionary_dbo.Times
-- Source: "Dictionary"."dbo"
-- Description: Reference data including PDS and lookup tables

select
    "SK_Time" as sk_time,
    "FullTime" as full_time,
    "FullTime12H" as full_time12_h,
    "Hours" as hours,
    "Hours12H" as hours12_h,
    "Minutes" as minutes,
    "TimeSuffex" as time_suffex,
    "IsMorning" as is_morning,
    "HoursName" as hours_name,
    "Hours12HName" as hours12_hname,
    "QuarterOfDay" as quarter_of_day,
    "QuarterOfDayName" as quarter_of_day_name,
    "QuarterOfDayNameShort" as quarter_of_day_name_short,
    "QuarterOfHour" as quarter_of_hour,
    "QuarterOfHourName" as quarter_of_hour_name,
    "QuarterOfHourShort" as quarter_of_hour_short
from {{ source('dictionary_dbo', 'Times') }}
