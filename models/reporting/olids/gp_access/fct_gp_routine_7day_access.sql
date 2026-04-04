{{
    config(
        materialized='table',
        tags=['fact', 'gp_access', 'kpi']
    )
}}

/*
GP Contract KPI: Routine appointment within 7 days

Percentage of non-clinically-urgent appointments where the patient was
seen within 7 days of booking.

Numerator: Routine attended appointments with days_to_appointment <= 7
Denominator: All routine attended appointments with a booking date
*/

select
    record_owner_organisation_code as practice_code,
    DATE_TRUNC('month', start_date) as report_month,
    COUNT(*) as routine_attended_total,
    SUM(CASE WHEN days_to_appointment <= 7 THEN 1 ELSE 0 END) as routine_within_7d,
    ROUND(
        100.0 * SUM(CASE WHEN days_to_appointment <= 7 THEN 1 ELSE 0 END) / NULLIF(COUNT(*), 0),
        1
    ) as routine_within_7d_pct
from {{ ref('int_appointment_gp_clean') }}
where urgency = 'Routine'
  and is_attended = TRUE
  and days_to_appointment IS NOT NULL
group by practice_code, report_month
