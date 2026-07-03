/*
OLIDS (NCL primary care) enrichment for the resource-to-need deep-dive, at
sk_patient_id grain.

Bridge: dim_person_pseudo maps OLIDS person_id -> sk_patient_id. Multiple
person_ids can share one sk_patient_id (fragmented patients), so every
measure is aggregated to sk grain here - never join person-grain OLIDS
models straight onto the sk-grain fact.

Measures (all over the SLAM-aligned rolling 12-month window where dated):
  * cambridge_comorbidity_score  - dim_person_ccms (max across siblings)
  * ltc_count / qof_ltc_count    - fct_person_ltc_summary register rows
  * frailty_severity             - fct_person_frailty_register, worst across
                                   siblings (Severe > Moderate > Mild)
  * gp_appointment_cost_12m      - int_appointment_gp_clinical attended
                                   appointments x PSSRU nominal unit cost.
                                   ADDITIVE to actual_cost_12m (GP activity
                                   is not in the SLAM/EPD spine).
  * prescribing_cost_12m         - EPD rows of fct_person_cost_by_month.
                                   A BREAKOUT of actual_cost_12m, not an
                                   addition. EPD trails SLAM (feed stalled) -
                                   prescribing_months_covered says how many
                                   window months EPD actually covers.
  * olids_prescribing_cost_12m_modelled - full-window prescribing from
                                   OLIDS estimated_cost
                                   (int_person_olids_prescribing_cost_12m).
                                   Use INSTEAD of the EPD breakout for
                                   whole-window primary-care cost.
*/

{{ config(materialized = 'table') }}

with bounds as (
    select
        max(activity_month)                as window_end,
        dateadd(month, -11, max(activity_month)) as window_start
    from {{ ref('fct_person_cost_by_month') }}
    where cost_source = 'SLAM'
),

bridge as (
    select person_id, sk_patient_id
    from {{ ref('dim_person_pseudo') }}
),

ccms as (
    select
        b.sk_patient_id,
        max(c.cambridge_comorbidity_score) as cambridge_comorbidity_score
    from {{ ref('dim_person_ccms') }} as c
    inner join bridge as b on c.person_id = b.person_id
    group by 1
),

ltc as (
    select
        b.sk_patient_id,
        count(distinct l.condition_code)                     as ltc_count,
        count(distinct iff(l.is_qof, l.condition_code, null)) as qof_ltc_count
    from {{ ref('fct_person_ltc_summary') }} as l
    inner join bridge as b on l.person_id = b.person_id
    group by 1
),

frailty as (
    select
        b.sk_patient_id,
        max(case f.latest_frailty_severity
            when 'Severe' then 3 when 'Moderate' then 2 when 'Mild' then 1
        end) as frailty_rank
    from {{ ref('fct_person_frailty_register') }} as f
    inner join bridge as b on f.person_id = b.person_id
    group by 1
),

gp_appts as (
    select
        b.sk_patient_id,
        sum(a.appointment_cost_gbp_nominal) as gp_appointment_cost_12m,
        count(*)                            as gp_appointments_12m
    from {{ ref('int_appointment_gp_clinical') }} as a
    inner join bridge as b on a.person_id = b.person_id
    cross join bounds
    where a.is_attended
      and a.start_date >= bounds.window_start
      and a.start_date < dateadd(month, 1, bounds.window_end)
    group by 1
),

olids_rx as (
    select
        b.sk_patient_id,
        sum(r.prescribing_cost_12m_modelled) as olids_prescribing_cost_12m_modelled,
        sum(r.prescription_orders_12m)       as olids_prescription_orders_12m
    from {{ ref('int_person_olids_prescribing_cost_12m') }} as r
    inner join bridge as b on r.person_id = b.person_id
    group by 1
),

epd as (
    select
        f.sk_patient_id,
        sum(f.total_cost) as prescribing_cost_12m
    from {{ ref('fct_person_cost_by_month') }} as f
    cross join bounds
    where f.cost_source = 'EPD'
      and f.activity_month between bounds.window_start and bounds.window_end
    group by 1
),

-- Feed coverage, not person property: window months EPD actually covers
epd_coverage as (
    select count(distinct f.activity_month) as prescribing_months_covered
    from {{ ref('fct_person_cost_by_month') }} as f
    cross join bounds
    where f.cost_source = 'EPD'
      and f.activity_month between bounds.window_start and bounds.window_end
),

sk_universe as (
    select distinct sk_patient_id from bridge
)

select
    u.sk_patient_id,
    ccms.cambridge_comorbidity_score,
    coalesce(ltc.ltc_count, 0)     as ltc_count,
    coalesce(ltc.qof_ltc_count, 0) as qof_ltc_count,
    case frailty.frailty_rank
        when 3 then 'Severe' when 2 then 'Moderate' when 1 then 'Mild'
    end                            as frailty_severity,
    coalesce(gp_appts.gp_appointment_cost_12m, 0) as gp_appointment_cost_12m,
    coalesce(gp_appts.gp_appointments_12m, 0)     as gp_appointments_12m,
    coalesce(epd.prescribing_cost_12m, 0)         as prescribing_cost_12m,
    cov.prescribing_months_covered,
    coalesce(olids_rx.olids_prescribing_cost_12m_modelled, 0) as olids_prescribing_cost_12m_modelled,
    coalesce(olids_rx.olids_prescription_orders_12m, 0)       as olids_prescription_orders_12m
from sk_universe as u
cross join epd_coverage as cov
left join ccms on u.sk_patient_id = ccms.sk_patient_id
left join ltc on u.sk_patient_id = ltc.sk_patient_id
left join frailty on u.sk_patient_id = frailty.sk_patient_id
left join gp_appts on u.sk_patient_id = gp_appts.sk_patient_id
left join epd on u.sk_patient_id = epd.sk_patient_id
left join olids_rx on u.sk_patient_id = olids_rx.sk_patient_id
