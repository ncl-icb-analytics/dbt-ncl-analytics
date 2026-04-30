with inclusion_list as (
    select olids_id, patient_id, area_code, 'inclusion' as source
    from {{ ref('cltcs_live_inclusion_cohort') }}
    where eligible = 1 and fragmented_sk_patient_id_flag = 0 and fragmented_person_id_flag = 0 -- exclude fragmented patients for now

),
dropped_list as ( -- people who were shortlisted but dropped out of inclusion cohort
    select arc.patient_id, arc.area_code, sl.olids_id, 'shortlist' as source
    from {{ ref('cltcs_inclusion_cohort_archive') }} arc
    inner join shortlisted_list sl on arc.patient_id = sl.patient_id
    where cohort_event = 'DELETE' and is_active = TRUE
),
excluded_list as ( -- no rereview or reconsideration within a year?
    select patient_id
    from {{ ref('cltcs_status_log_most_recent') }}
    where action in ('Reject', 'Case closed') 
    and action_date is between dateadd(year, -1, current_date()) and current_date()
),
complete_list as (
    select * from inclusion_list
    union all
    select * from dropped_list
)
select olids_id, patient_id, area_code
from complete_list
where patient_id is not in (select patient_id from excluded_list)
qualify row_number() over (
    partition by patient_id
    order by case when source = 'shortlist' then 0 else 1 end
) = 1
