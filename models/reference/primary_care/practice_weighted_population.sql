-- Practice need-weighted population by allocation base year, from the NHS
-- England revenue allocations practice-level workbooks (Core WP.xlsx) via
-- UKHFD. All values, including registered population, are modelled
-- projections for the allocation base year, not actual list counts - use
-- practice_registered_patients for actuals. weighted_patients is the mean of
-- the six service-specific weighted populations: G&A, community, mental
-- health, maternity, prescribing and primary medical care. Health
-- inequalities and the supplied Core Services aggregate are not included in
-- that mean.
{{ config(materialized = 'table') }}

with metrics as (
    select
        practice_code,
        financial_year_start,
        source_file_version,
        metric_name,
        metric_value
    from {{ ref('stg_ukhfd_weighted_regs_by_gp_practice_base') }}
    where metric_name in (
        'Registered population (base year)',
        'General and Acute (G&A) weighted population',
        'Community Services (CS) weighted population',
        'Mental Health (MH) weighted population',
        'Maternity weighted population',
        'Prescribing weighted population',
        'Primary Medical Care weighted population'
    )
),

practice_year as (
    select
        practice_code,
        financial_year_start,
        max(source_file_version) as source_file_version,
        max(iff(metric_name = 'Registered population (base year)', metric_value, null))
            as registered_patients,
        avg(iff(metric_name != 'Registered population (base year)', metric_value, null))
            as weighted_patients,
        count_if(metric_name != 'Registered population (base year)')
            as weighted_component_count
    from metrics
    group by practice_code, financial_year_start
)

select
    s.practice_code,
    concat(
        year(s.financial_year_start),
        '/',
        right((year(s.financial_year_start) + 1)::varchar, 2)
    ) as financial_year,
    s.financial_year_start,
    s.registered_patients,
    s.weighted_patients,
    div0(s.weighted_patients, s.registered_patients) as weighted_to_registered_ratio,
    s.weighted_component_count,
    s.source_file_version,
    s.financial_year_start = max(s.financial_year_start)
        over (partition by s.practice_code) as is_latest
from practice_year as s
where s.registered_patients > 0
