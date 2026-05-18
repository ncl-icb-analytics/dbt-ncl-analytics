with registrant_list as (
    select patient_id, area_code, olids_id, 'registrant' as source
    from {{ ref('cltcs_registrant_adult_population') }}
    where fragmented_sk_patient_id_flag = 0 and fragmented_person_id_flag = 0 -- exclude fragmented patients for now
),
-- dropped_list as ( -- people who were shortlisted but dropped out of inclusion cohort
--     select arc.patient_id, arc.area_code, pp.person_id as olids_id, 'shortlist' as source
--     from {{ ref('stg_c_ltcs_inclusion_cohort_archive') }} arc
--     left join {{ref('dim_person_pseudo')}} pp on pp.sk_patient_id = arc.patient_id
--     where cohort_event = 'DELETE' and is_active = TRUE
--     qualify row_number() over (partition by patient_id order by event_written_at desc) = 1
-- ),
shortlisted_list as (
    select sl.patient_id, sl.area_code, pp.person_id as olids_id, 'shortlist' as source
    from {{ ref('cltcs_status_log_most_recent') }} sl
    left join {{ref('dim_person_pseudo')}} pp on pp.sk_patient_id = sl.patient_id
    where action not in ('Reject', 'Case closed', 'Removed from shortlist') 
),
excluded_list as ( -- no rereview or reconsideration within a year?
    select patient_id
    from {{ ref('cltcs_status_log_most_recent') }} 
    where action in ('Reject', 'Case closed') 
    and action_date between dateadd(year, -1, current_date()) and current_date()
),
complete_list as (
    select * from registrant_list
    union all
    select * from shortlisted_list
),

expanded_registrant_list as (
    select patient_id, area_code, olids_id, source
    from complete_list c
    where not exists (
    select 1
    from excluded_list e
    where e.patient_id = c.patient_id)
    qualify row_number() over (
        partition by patient_id
        order by case when source = 'shortlist' then 0 else 1 end
    ) = 1)

select erl.patient_id
    , erl.area_code
    , erl.olids_id
    , erl.source
    , pc.total_conditions
    , fr.category 
    , ccms.cambridge_comorbidity_score
    , zeroifnull(aea.ae_tot_12mo) as ae_tot_12mo
    , lcs.overall_risk_group
    , rm.unique_active_ingredient_count_12mo
from expanded_registrant_list erl
left join {{ ref('dim_person_conditions')}} pc
    on erl.olids_id = pc.person_id
left join {{ref('fct_person_sus_ae_recent')}} aea
    on erl.patient_id  = aea.sk_patient_id
left join {{ref('fct_person_medications_recent')}} rm
    on erl.olids_id = rm.person_id
left join {{ref('stg_aic_int_efi2_scores')}} fr
    on erl.olids_id = fr.person_id
left join {{ref('stg_aic_int_ccms_current')}} ccms
    on erl.olids_id = ccms.person_id
left join {{ref('fct_person_ltc_lcs_risk_summary')}} lcs
    on erl.olids_id = lcs.person_id
where
    erl.source = 'shortlist' or
    (pc.total_conditions > 2 
    and (ccms.cambridge_comorbidity_score > 1.5
        or fr.category in ('MODERATE FRAILTY', 'SEVERE FRAILTY')
        or lcs.overall_risk_group in ('HRC', 'HR')))