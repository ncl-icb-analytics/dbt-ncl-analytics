{{
    config(
        description="Raw layer (Reference data including PDS and lookup tables). 1:1 passthrough with cleaned column names. \nSource: Dictionary.dbo.Dates_Month_Year \ndbt: source(''dictionary_dbo'', ''Dates_Month_Year'') \nColumns:\n  SK_YearMonth -> sk_year_month\n  MonthNum -> month_num\n  MonthName -> month_name\n  CalendarMonthNumber -> calendar_month_number\n  CalendarYearNumber -> calendar_year_number\n  StartOfMonthDate -> start_of_month_date\n  EndOfMonthDate -> end_of_month_date\n  SK_Date_StartOfMonth -> sk_date_start_of_month\n  SK_Date_EndOfMonth -> sk_date_end_of_month"
    )
}}
select
    "SK_YearMonth" as sk_year_month,
    "MonthNum" as month_num,
    "MonthName" as month_name,
    "CalendarMonthNumber" as calendar_month_number,
    "CalendarYearNumber" as calendar_year_number,
    "StartOfMonthDate" as start_of_month_date,
    "EndOfMonthDate" as end_of_month_date,
    "SK_Date_StartOfMonth" as sk_date_start_of_month,
    "SK_Date_EndOfMonth" as sk_date_end_of_month
from {{ source('dictionary_dbo', 'Dates_Month_Year') }}
