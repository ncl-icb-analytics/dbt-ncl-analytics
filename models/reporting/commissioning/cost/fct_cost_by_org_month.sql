/*
SLAM commissioned cost by organisation and month — reporting aggregate for
the Aligning Resource to Need work.

Grain: activity_month x icb_group x commissioner_code x provider_code x
service_grouping x service. Built from int_slam_activity_cost (SLAM actual
cost spine, rolling 12 months).

icb_group splits WNL-commissioned (resident) spend from out-of-sector
pass-through (other ICBs' residents treated at WNL providers). Use
in_patch = true for the resource-to-need numerator.

Measures: total_cost / total_activity, plus *_attributable variants
(is_patient_attributable lines only) so per-patient and need cuts exclude
block/adjustment/transport.

Prescribing (EPD) is patient-level NCL only with no commissioner/provider —
it is not included here; see fct_person_cost_by_month for the patient-level
SLAM+EPD union.
*/

select
    activity_month
    , case
        when commissioner_code in ('W2U3Z', '93C', 'Z9B2Z') then 'WNL in-patch'
        else 'Out-of-sector'
      end                                               as icb_group
    , commissioner_code in ('W2U3Z', '93C', 'Z9B2Z')    as in_patch
    , commissioner_code
    , provider_code
    , service_grouping
    , service
    , sum(total_cost)                                   as total_cost
    , sum(total_activity)                               as total_activity
    , sum(iff(is_patient_attributable, total_cost, 0))  as attributable_cost
    , sum(iff(is_patient_attributable, total_activity, 0)) as attributable_activity
    , count(distinct sk_patient_id)                     as patients
from {{ ref('int_slam_activity_cost') }}
group by
    activity_month
    , icb_group
    , in_patch
    , commissioner_code
    , provider_code
    , service_grouping
    , service
