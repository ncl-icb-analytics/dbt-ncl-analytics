-- Staging model for dictionary_dbo.Times
-- Source: "Dictionary"."dbo"
-- Description: Reference data including PDS and lookup tables

select
    "SK_Time" as sk_time,
    "FullTime" as fulltime,
    "FullTime12H" as fulltime12h,
    "Hours" as hours,
    "Hours12H" as hours12h,
    "Minutes" as minutes,
    "TimeSuffex" as timesuffex,
    "IsMorning" as ismorning,
    "HoursName" as hoursname,
    "Hours12HName" as hours12hname,
    "QuarterOfDay" as quarterofday,
    "QuarterOfDayName" as quarterofdayname,
    "QuarterOfDayNameShort" as quarterofdaynameshort,
    "QuarterOfHour" as quarterofhour,
    "QuarterOfHourName" as quarterofhourname,
    "QuarterOfHourShort" as quarterofhourshort
from {{ source('dictionary_dbo', 'Times') }}
