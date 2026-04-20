with inclusion_list as (
    select olids_id, patient_id, area_code, 'inclusion' as source
    from {{ ref('cltcs_live_inclusion_cohort') }}
    where eligible = 1 and fragmented_sk_patient_id_flag = 0 and fragmented_person_id_flag = 0 -- exclude fragmented patients for now

),
shortlisted_list as (
    select pp.person_id as olids_id, sl.patient_id, sl.area_code, 'shortlist' as source
    from {{ ref('cltcs_status_log_most_recent') }} sl
    left join {{ ref('dim_person_pseudo') }} pp on pp.sk_patient_id = sl.patient_id
    where action in ('Added to shortlist', 'Accept for MDT')
),
complete_list as (
    select * from inclusion_list
    union all
    select * from shortlisted_list
)
select olids_id, patient_id, area_code
from complete_list
qualify row_number() over (
    partition by patient_id
    order by case when source = 'shortlist' then 0 else 1 end
) = 1
