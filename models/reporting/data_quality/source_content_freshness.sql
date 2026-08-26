{{
    config(
        materialized='table',
        tags=['daily']
    )
}}

/* Analyst-facing source content-currency contract. */

with source_signals as (
    select *
    from {{ ref('stg_source_content_freshness') }}
),

ages as (
    select
        *,
        datediff(day, content_date, current_date()) as content_age_days,
        dateadd(day, expected_days, content_date)::date as expected_by_date
    from source_signals
),

analyst_signals as (
    select
        *,
        greatest(content_age_days - expected_days, 0) as days_past_expected,
        breach_after_days - content_age_days as days_until_breach,
        case
            when content_date is null then 'unknown'
            when content_age_days <= expected_days then 'fresh'
            when sla_days is not null and content_age_days <= sla_days then 'within_sla'
            when content_age_days <= breach_after_days then 'late'
            else 'breach'
        end as freshness_status
    from ages
)

select *
from analyst_signals
