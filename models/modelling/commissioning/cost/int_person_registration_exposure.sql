/*
WNL registration exposure over the cost window — the population base for the
Aligning Resource to Need model.

One row per patient with >= 1 month of WNL registration during the rolling
12-month cost window (aligned to fct_person_cost_by_month). Replaces the
current-registration flag as the population basis so the deceased and the
deducted (moved away / deregistered) are included: filtering to
flag_current_registered drops ~17% of in-patch patient-keyed spend (decedents
average ~10x the per-head cost of survivors), which biases resource-to-need
against areas with older populations.

Method: PDS registration episodes (stg_pds_patient_care_practice, full
history) x WNL practices (dim_practice), expanded to a 12-month spine. A month
counts when an episode covers it AND the patient has not died before it (PDS
does not close registration episodes at death, so exposure is truncated at the
date_of_death month — the death month itself counts). Patients who died before
the window start get no months and drop out.

practice_code is the practice at the patient's last registered month in the
window (max_by), so leavers/decedents attribute to where they were registered,
not their current record.
*/
{{ config(materialized = 'table') }}

with bounds as (
    select
        max(activity_month)                as end_month,
        dateadd(month, -11, max(activity_month)) as start_month
    from {{ ref('fct_person_cost_by_month') }}
),

months as (
    select dateadd(month, s.seq, b.start_month) as reg_month
    from (
        select row_number() over (order by null) - 1 as seq
        from table(generator(rowcount => 12))
    ) as s
    cross join bounds as b
),

wnl_episodes as (
    select
        e.sk_patient_id,
        e.event_from_date,
        coalesce(e.event_to_date, '9999-12-31'::date) as event_to_date,
        e.practice_code,
        e.row_id
    from {{ ref('stg_pds_patient_care_practice') }} as e
    join {{ ref('dim_practice') }} as d
        on e.practice_code = d.practice_code
        and d.is_wnl_practice
),

person as (
    select sk_patient_id, date_of_death
    from {{ ref('int_person_pds_latest_record') }}
),

-- one row per patient-month covered by an episode; latest-starting episode
-- wins the month's practice attribution
person_months as (
    select
        e.sk_patient_id,
        m.reg_month,
        e.practice_code
    from wnl_episodes as e
    join months as m
        on e.event_from_date <= last_day(m.reg_month)
        and e.event_to_date >= m.reg_month
    qualify row_number() over (
        partition by e.sk_patient_id, m.reg_month
        order by e.event_from_date desc, e.row_id desc
    ) = 1
),

alive_months as (
    select
        pm.sk_patient_id,
        pm.reg_month,
        pm.practice_code,
        p.date_of_death
    from person_months as pm
    left join person as p
        on p.sk_patient_id = pm.sk_patient_id
    -- exposure runs to the death month inclusive
    where p.date_of_death is null
       or date_trunc(month, p.date_of_death) >= pm.reg_month
)

select
    a.sk_patient_id,
    count(*)                                as months_registered,
    min(a.reg_month)                        as first_month_registered,
    max(a.reg_month)                        as last_month_registered,
    max_by(a.practice_code, a.reg_month)    as practice_code,
    any_value(a.date_of_death)              as date_of_death,
    boolor_agg(coalesce(
        a.date_of_death between b.start_month and last_day(b.end_month), false
    ))                                      as died_in_window,
    any_value(b.end_month)                  as window_end_month
from alive_months as a
cross join bounds as b
group by a.sk_patient_id
