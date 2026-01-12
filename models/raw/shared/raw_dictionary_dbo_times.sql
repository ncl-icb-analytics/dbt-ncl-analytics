{{
    config(
        description="Raw layer (Reference data including PDS and lookup tables). 1:1 passthrough with cleaned column names. \nSource: Dictionary.dbo.Times \ndbt: source(''dictionary_dbo'', ''Times'') \nColumns:\n  SK_Time -> sk_time\n  FullTime -> full_time\n  FullTime12H -> full_time12_h\n  Hours -> hours\n  Hours12H -> hours12_h\n  Minutes -> minutes\n  TimeSuffex -> time_suffex\n  IsMorning -> is_morning\n  HoursName -> hours_name\n  Hours12HName -> hours12_hname\n  QuarterOfDay -> quarter_of_day\n  QuarterOfDayName -> quarter_of_day_name\n  QuarterOfDayNameShort -> quarter_of_day_name_short\n  QuarterOfHour -> quarter_of_hour\n  QuarterOfHourName -> quarter_of_hour_name\n  QuarterOfHourShort -> quarter_of_hour_short"
    )
}}
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
