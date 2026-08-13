{{
    config(
        description="Raw layer (UKHD reference and dimension tables). 1:1 passthrough with cleaned column names. \nSource: UKHFD.dbo.dim_ref_Time \ndbt: source(''ukhfd'', ''dim_ref_Time'') \nColumns:\n  Time_Key -> time_key\n  Time -> time\n  Time24 -> time24\n  HourName -> hour_name\n  MinuteName -> minute_name\n  Hour -> hour\n  Hour_2Char -> hour_2_char\n  Hour24 -> hour24\n  Hour24_2Char -> hour24_2_char\n  Minute -> minute\n  Minute_2Char -> minute_2_char\n  Second -> second\n  Second_2Char -> second_2_char\n  AM -> am\n  Morning_Or_Afternoon -> morning_or_afternoon\n  Business_Hours_09To17 -> business_hours_09_to17\n  Business_Hours_08To18 -> business_hours_08_to18\n  Daytime_Hours_07To23 -> daytime_hours_07_to23\n  Qtr_Of_Day -> qtr_of_day\n  Qtr_Hr_Of_Day -> qtr_hr_of_day\n  Qtr_Hrs -> qtr_hrs\n  Qtr_of_Hr -> qtr_of_hr\n  Created_Date -> created_date\n  Import_Date -> import_date"
    )
}}
select
    "Time_Key" as time_key,
    "Time" as time,
    "Time24" as time24,
    "HourName" as hour_name,
    "MinuteName" as minute_name,
    "Hour" as hour,
    "Hour_2Char" as hour_2_char,
    "Hour24" as hour24,
    "Hour24_2Char" as hour24_2_char,
    "Minute" as minute,
    "Minute_2Char" as minute_2_char,
    "Second" as second,
    "Second_2Char" as second_2_char,
    "AM" as am,
    "Morning_Or_Afternoon" as morning_or_afternoon,
    "Business_Hours_09To17" as business_hours_09_to17,
    "Business_Hours_08To18" as business_hours_08_to18,
    "Daytime_Hours_07To23" as daytime_hours_07_to23,
    "Qtr_Of_Day" as qtr_of_day,
    "Qtr_Hr_Of_Day" as qtr_hr_of_day,
    "Qtr_Hrs" as qtr_hrs,
    "Qtr_of_Hr" as qtr_of_hr,
    "Created_Date" as created_date,
    "Import_Date" as import_date
from {{ source('ukhfd', 'dim_ref_Time') }}
