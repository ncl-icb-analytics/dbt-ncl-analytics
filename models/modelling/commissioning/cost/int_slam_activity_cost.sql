/*
Patient-level SLAM activity & actual cost — the spine of the Aligning
Resource to Need model.

Grain: sk_patient_id x activity_month x commissioner x provider x
service hierarchy (service_grouping > service > activity_type) x POD x HRG.

Source: stg_lsplcm_latest (latest-submission LSPLCM PLD — actual provider
cost dv_total_cost, NOT national tariff). Covers the WNL footprint
(NCL + NWL merged). Rolling most-recent 12 months by financial period.

Cost basis: SLAM actual cost is the agreed spine over SUS tariff — it is
real commissioned spend (local prices, CQUIN, top-ups), includes high-cost
drugs/devices that tariff excludes, and is commissioner-attributed.

POD -> service hierarchy via the slam_pod_service_grouping seed (joined on
upper-cased POD). is_patient_attributable carries through so per-patient /
need cuts can exclude block/adjustment/transport lines while provider
totals keep them.

Period: dv_financial_year 'YYYYYY' + dv_financial_month (1=April..12=March)
converted to the first of the calendar month. Rows with no parsed financial
period are dropped (period unknown).
*/

with base as (
    select
        s.sk_patient_id
        , date_from_parts(
            left(s.dv_financial_year, 4)::int + iff(s.dv_financial_month >= 10, 1, 0)
            , mod(s.dv_financial_month + 2, 12) + 1
            , 1
          )                                             as activity_month
        , s.commissioner_code
        , s.dv_provider_code                            as provider_code
        -- responsibility (not registration) code is the populated practice
        -- field in current SLAM: gp_practice_code_registration is ~0%,
        -- gp_practice_responsibility_code ~98%.
        , s.gp_practice_responsibility_code             as registered_practice_code
        , upper(trim(s.point_of_delivery_code))         as pod_code
        , s.tariff_code                                 as hrg_code
        , s.age_at_activity_date
        , s.gender_code
        , s.ethnic_category
        , s.lsoa
        , s.dv_total_cost
        , s.dv_activity_count
    from {{ ref('stg_lsplcm_latest') }} as s
    where s.dv_financial_year is not null
      and s.dv_financial_month between 1 and 12
)

-- Anchor the rolling window on the latest month with material volume, not
-- max(activity_month): a handful of mis-stated rows carry future financial
-- periods (e.g. 2027-03) that would otherwise shrink the window. Months with
-- a real submission have ~1.7M rows; junk/partial months have <1k.
, bounds as (
    select max(activity_month) as end_month
    from (
        select activity_month
        from base
        group by activity_month
        having count(*) > 100000
    )
)

, recent as (
    select b.*
    from base as b
    cross join bounds
    where b.activity_month > dateadd('month', -12, bounds.end_month)
      and b.activity_month <= bounds.end_month
)

select
    r.sk_patient_id
    , r.activity_month
    , r.commissioner_code
    , r.provider_code
    , r.registered_practice_code
    -- service hierarchy (POD seed); unmatched -> Other / Unmapped
    , coalesce(m.service_grouping, 'Unmapped')          as service_grouping
    , coalesce(m.service, 'Unmapped')                   as service
    , coalesce(m.activity_type, 'Other')                as activity_type
    , r.pod_code
    , r.hrg_code
    , coalesce(m.is_patient_attributable, false)        as is_patient_attributable
    -- demographics at event (constant within patient-month; any_value safe)
    , any_value(r.age_at_activity_date)                 as age_at_activity_date
    , any_value(r.gender_code)                          as gender_code
    , any_value(r.ethnic_category)                      as ethnic_category
    , any_value(r.lsoa)                                 as lsoa
    -- measures
    , sum(r.dv_total_cost)                              as total_cost
    , sum(r.dv_activity_count)                          as total_activity
    , count(*)                                          as line_count
from recent as r
left join {{ ref('slam_pod_service_grouping') }} as m
    on m.pod_code = r.pod_code
group by
    r.sk_patient_id
    , r.activity_month
    , r.commissioner_code
    , r.provider_code
    , r.registered_practice_code
    , service_grouping
    , service
    , activity_type
    , r.pod_code
    , r.hrg_code
    , is_patient_attributable
