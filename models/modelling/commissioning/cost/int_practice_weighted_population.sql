-- Carr-Hill weighted population per WNL practice per financial year, from
-- NHS Payments to General Practice (annual, FY 2015/16 onwards). The need
-- denominator for the weighted resource-to-need basis.
--
-- weighted_to_registered_ratio > 1 = the practice's population needs more
-- GP resource than its raw list implies (Carr-Hill: age-sex workload,
-- morbidity/mortality, care-home residence, turnover, rurality, MFF).
--
-- is_latest flags each practice's most recent available year (not the
-- global max) so merged/closed practices keep their last published ratio.
{{ config(materialized = 'table') }}

select
    s.practice_code,
    s.financial_year,
    s.financial_year_start,
    s.registered_patients,
    s.weighted_patients,
    div0(s.weighted_patients, s.registered_patients) as weighted_to_registered_ratio,
    s.financial_year_start = max(s.financial_year_start)
        over (partition by s.practice_code)          as is_latest
from {{ ref('stg_nhs_payments_gp_practice_payments') }} as s
inner join {{ ref('dim_practice') }} as p
    on s.practice_code = p.practice_code
    and p.is_wnl_practice
