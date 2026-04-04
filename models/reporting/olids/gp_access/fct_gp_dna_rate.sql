{{
    config(
        materialized='table',
        tags=['fact', 'gp_access', 'kpi']
    )
}}

/*
GP appointment DNA (Did Not Attend) rate

Percentage of booked appointments where the patient did not attend.

Numerator: Appointments with is_dna = TRUE
Denominator: All appointments (attended + DNA, excluding cancelled/future)

DNA rates vary significantly by deprivation and demographics. Join to
practice-level demographics for equity analysis.
*/

select
    record_owner_organisation_code as practice_code,
    DATE_TRUNC('month', start_date) as report_month,
    COUNT(*) as total_appointments,
    SUM(CASE WHEN is_attended THEN 1 ELSE 0 END) as attended,
    SUM(CASE WHEN is_dna THEN 1 ELSE 0 END) as dna,
    ROUND(
        100.0 * SUM(CASE WHEN is_dna THEN 1 ELSE 0 END) / NULLIF(COUNT(*), 0),
        1
    ) as dna_rate_pct
from {{ ref('int_appointment_gp_clean') }}
group by practice_code, report_month
