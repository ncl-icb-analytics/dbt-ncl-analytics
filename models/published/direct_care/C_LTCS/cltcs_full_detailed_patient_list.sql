with inclusion_list as (
    select olids_id, patient_id, pcn_code, 'inclusion' as source
    from {{ ref('inclusion_cohort') }}
    where eligible = 1
),
shortlisted_list as (
    select pp.person_id as olids_id, sl.patient_id, sl.pcn_code, 'shortlist' as source
    from {{ ref('cltcs_status_log_most_recent') }} sl
    left join {{ ref('dim_person_pseudo') }} pp on pp.sk_patient_id = sl.patient_id
    where action in ('Added to shortlist', 'Accept for MDT')
),
complete_list as (
    select * from inclusion_list
    union all
    select * from shortlisted_list
)
select olids_id, patient_id, pcn_code
from complete_list
qualify row_number() over (
    partition by olids_id, patient_id
    order by case when source = 'shortlist' then 0 else 1 end
) = 1
