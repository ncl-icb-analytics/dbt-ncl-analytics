-- Raw layer model for reference_analyst_managed.LAST_SUNDAY_OF_THE_MONTH
-- Source: "DATA_LAKE__NCL"."ANALYST_MANAGED"
-- Description: Analyst-managed reference datasets and business rules
-- This is a 1:1 passthrough from source with standardized column names
select
    "LastSundayOfMonthDate" as last_sunday_of_month_date,
    "MonthName" as month_name,
    "CalendarYearNumber" as calendar_year_number,
    "FiscalCalendarYearName" as fiscal_calendar_year_name,
    "FiscalCalendarMonthNameYM" as fiscal_calendar_month_name_ym,
    "StartOfMonthDate" as start_of_month_date,
    "EndOfMonthDate" as end_of_month_date
from {{ source('reference_analyst_managed', 'LAST_SUNDAY_OF_THE_MONTH') }}
