/*
Patient-level spend by month and service grouping — canonical cost index for
WNL resource analysis.

Grain: sk_patient_id x activity_month x service_grouping x service x
is_patient_attributable x cost_basis x cost_source x activity_unit.

Full-history union of the two cost spines built so far:
  * SLAM (int_cost_index_slam_activity_monthly) — acute / community actual cost, WNL
    (NCL + NWL). From 2021-04 to the last complete month.
  * EPD prescribing (int_cost_index_epd_prescribing_monthly) — GP prescriptions
    (Community), WNL. From 2018-04 and about 12 months behind SLAM.

Full history at every layer here — no rolling window. The 12-month analysis
window is applied in reporting facts (fct_person_resource_index).
cost_basis distinguishes actual cost from future proxy/nominal sources.

Still to union (sources identified, build pending): GP appointments (OLIDS),
MH inpatient + contacts (MHSDS — needs the discharge-forward dedup fix),
community contacts (CSDS), high-cost drugs/devices PLD (LSDrPLCM/LSDePLCM).

is_patient_attributable carries through from the POD mapping — filter it for
per-patient resource cuts (excludes SLAM block/adjustment lines);
keep all rows for provider/system totals.
*/

with slam as (
    select
        sk_patient_id
        , activity_month
        , service_grouping
        , service
        , is_patient_attributable
        , cost_basis
        , 'SLAM' as cost_source
        , 'slam_activity' as activity_unit
        , total_cost
        , total_activity
    from {{ ref('int_cost_index_slam_activity_monthly') }}
)


, rx as (
    select
        p.sk_patient_id
        , p.activity_month
        , 'Community'        as service_grouping
        , 'GP Prescriptions' as service
        , true               as is_patient_attributable
        , p.cost_basis
        , 'EPD'              as cost_source
        , 'prescription_item' as activity_unit
        , p.prescribing_cost as total_cost
        , p.prescribing_items as total_activity
    from {{ ref('int_cost_index_epd_prescribing_monthly') }} as p
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
    , cost_basis
    , cost_source
    , activity_unit
    , sum(total_cost)       as total_cost
    , sum(total_activity)   as total_activity
from combined
group by
    sk_patient_id
    , activity_month
    , service_grouping
    , service
    , is_patient_attributable
    , cost_basis
    , cost_source
    , activity_unit
