{{
    config(
        description="Raw layer (Analyst-managed reference datasets and business rules). 1:1 passthrough with cleaned column names. \nSource: DATA_LAKE__NCL.ANALYST_MANAGED.LAST_SUNDAY_OF_THE_MONTH \ndbt: source(''reference_analyst_managed'', ''LAST_SUNDAY_OF_THE_MONTH'') \nColumns:\n  LastSundayOfMonthDate -> last_sunday_of_month_date\n  MonthName -> month_name\n  CalendarYearNumber -> calendar_year_number\n  FiscalCalendarYearName -> fiscal_calendar_year_name\n  FiscalCalendarMonthNameYM -> fiscal_calendar_month_name_ym\n  StartOfMonthDate -> start_of_month_date\n  EndOfMonthDate -> end_of_month_date"
    )
}}
select
    "LastSundayOfMonthDate" as last_sunday_of_month_date,
    "MonthName" as month_name,
    "CalendarYearNumber" as calendar_year_number,
    "FiscalCalendarYearName" as fiscal_calendar_year_name,
    "FiscalCalendarMonthNameYM" as fiscal_calendar_month_name_ym,
    "StartOfMonthDate" as start_of_month_date,
    "EndOfMonthDate" as end_of_month_date
from {{ source('reference_analyst_managed', 'LAST_SUNDAY_OF_THE_MONTH') }}
