/*
Patient-level spend by month and service grouping — headline fact for the
Aligning Resource to Need analysis.

Grain: sk_patient_id x activity_month x service_grouping x service x
cost_source.

Unions the two cost spines built so far:
  * SLAM (int_slam_activity_cost) — acute / community actual cost, WNL
    (NCL + NWL). The agreed spine over SUS tariff.
  * EPD prescribing (int_person_prescribing_cost_monthly) — GP prescriptions
    (Community), WNL.

Prescribing is restricted to the SLAM rolling-12-month window so both
sources cover the same period. The EPD feed lags (~12 months behind as of
2026-07), so recent window months carry SLAM cost only — the
epd_covers_slam_cost_window test (warn) flags the gap and will surface the
step-change when the feed catches up.

Still to union (sources identified, build pending): GP appointments (OLIDS),
MH inpatient + contacts (MHSDS — needs the discharge-forward dedup fix),
community contacts (CSDS), high-cost drugs/devices PLD (LSDrPLCM/LSDePLCM).

is_patient_attributable carries through from the POD mapping — filter it for
per-patient / spend-vs-need cuts (excludes SLAM block/adjustment lines);
keep all rows for provider/system totals.
*/

with slam as (
    select
        sk_patient_id
        , activity_month
        , service_grouping
        , service
        , is_patient_attributable
        , 'SLAM' as cost_source
        , total_cost
        , total_activity
    from {{ ref('int_slam_activity_cost') }}
)

, window_bounds as (
    select min(activity_month) as min_month, max(activity_month) as max_month
    from slam
)

, rx as (
    select
        p.sk_patient_id
        , p.activity_month
        , 'Community'        as service_grouping
        , 'GP Prescriptions' as service
        , true               as is_patient_attributable
        , 'EPD'              as cost_source
        , p.prescribing_cost as total_cost
        , p.prescribing_items as total_activity
    from {{ ref('int_person_prescribing_cost_monthly') }} as p
    cross join window_bounds as w
    where p.activity_month between w.min_month and w.max_month
)

, combined as (
    select * from slam
    union all
    select * from rx
)

select
    sk_patient_id
    , activity_month
    , service_grouping
    , service
    , is_patient_attributable
    , cost_source
    , sum(total_cost)       as total_cost
    , sum(total_activity)   as total_activity
from combined
group by
    sk_patient_id
    , activity_month
    , service_grouping
    , service
    , is_patient_attributable
    , cost_source
