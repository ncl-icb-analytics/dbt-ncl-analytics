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
emis_list as (
    select ed.patient_id, ed.area_code, pp.person_id as olids_id,'emis' as source
    from {{ ref('cltcs_emis_data') }} ed
    left join {{ref('dim_person_pseudo')}} pp on pp.sk_patient_id = ed.patient_id
),
complete_list as (
    select * from registrant_list
    union all
    select * from shortlisted_list
    union all
    select * from emis_list
),

expanded_registrant_list as (
    select patient_id
        , area_code
        , olids_id
        , source
    from complete_list c
    -- where not exists (
    -- select 1
    -- from excluded_list e
    -- where e.patient_id = c.patient_id)
    qualify row_number() over (
        partition by patient_id
        order by case
            when source = 'emis' then 0
            when source = 'shortlist' then 1
            when source = 'registrant' then 2
            else 3
        end
    ) = 1)

select erl.patient_id
    , erl.area_code
    , erl.olids_id
    , erl.source
    , case when exists (select 1 from shortlisted_list sl where sl.patient_id = erl.patient_id) then 1 else 0 end as shortlist_flag
    , case when exists (select 1 from emis_list el where el.patient_id = erl.patient_id) then 1 else 0 end as emis_flag
    , case when exists (select 1 from registrant_list rl where rl.patient_id = erl.patient_id) then 1 else 0 end as registrant_flag
    , pc.total_conditions
    , fr.category 
    , ccms.cambridge_comorbidity_score
    , zeroifnull(aea.ae_tot_12mo) as ae_tot_12mo
    , lcs.overall_risk_group
    , rm.unique_active_ingredient_count_12mo
    , frr.latest_frailty_severity
from expanded_registrant_list erl
left join {{ ref('dim_person_conditions')}} pc
    on erl.olids_id = pc.person_id
left join {{ref('fct_person_sus_uec_recent')}} aea
    on erl.patient_id  = aea.sk_patient_id
left join {{ref('fct_person_sus_apc_recent')}} apca
    on erl.patient_id  = apca.sk_patient_id
left join {{ref('fct_person_medications_recent')}} rm
    on erl.olids_id = rm.person_id
left join {{ref('fct_person_efi2')}} fr
    on erl.olids_id = fr.person_id
left join {{ref('fct_person_frailty_register')}} frr
    on erl.olids_id = frr.person_id
left join {{ref('dim_person_ccms')}} ccms
    on erl.olids_id = ccms.person_id
left join {{ref('fct_person_ltc_lcs_risk_summary')}} lcs
    on erl.olids_id = lcs.person_id
where
    erl.source in ('shortlist', 'emis') or
    (pc.total_conditions > 2 
    and (
        -- clinical complexity
        ccms.cambridge_comorbidity_score > 1.5
        or lcs.overall_risk_group in ('HRC', 'HR')
        -- frailty
        or fr.category in ('MODERATE FRAILTY', 'SEVERE FRAILTY')
        or (frr.moderate_frailty_count > 0 or frr.severe_frailty_count > 0)
        -- emergency use
        or aea.ae_tot_12mo > 3 -- 4+ AE visits within 12 months
        or apca.acs_nel_12mo > 1 -- 2+ NEL for ASC conditions within 12 months
        )
        )