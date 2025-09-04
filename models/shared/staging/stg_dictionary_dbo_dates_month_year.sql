-- Staging model for dictionary_dbo.Dates_Month_Year
-- Source: "Dictionary"."dbo"
-- Description: Reference data including PDS and lookup tables

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
