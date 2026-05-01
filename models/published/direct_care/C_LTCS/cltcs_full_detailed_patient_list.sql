with inclusion_list as (
    select patient_id, area_code, olids_id, 'inclusion' as source
    from {{ ref('cltcs_live_inclusion_cohort') }}
    where eligible = 1 and fragmented_sk_patient_id_flag = 0 and fragmented_person_id_flag = 0 -- exclude fragmented patients for now

),
dropped_list as ( -- people who were shortlisted but dropped out of inclusion cohort
    select arc.patient_id, arc.area_code, pp.person_id as olids_id, 'shortlist' as source
    from {{ ref('stg_c_ltcs_inclusion_cohort_archive') }} arc
    left join {{ref('dim_person_pseudo')}} pp on pp.sk_patient_id = arc.patient_id
    where cohort_event = 'DELETE' and is_active = TRUE
    qualify row_number() over (partition by patient_id order by event_written_at desc) = 1
),
excluded_list as ( -- no rereview or reconsideration within a year?
    select patient_id
    from {{ ref('cltcs_status_log_most_recent') }}
    where action in ('Reject', 'Case closed') 
    and action_date between dateadd(year, -1, current_date()) and current_date()
),
complete_list as (
    select * from inclusion_list
    union all
    select * from dropped_list
)

select patient_id, area_code, olids_id, source
from complete_list c
where not exists (
  select 1
  from excluded_list e
  where e.patient_id = c.patient_id)
qualify row_number() over (
    partition by patient_id
    order by case when source = 'shortlist' then 0 else 1 end
) = 1
