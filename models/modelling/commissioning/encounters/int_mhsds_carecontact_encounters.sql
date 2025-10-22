/*
Mental Health Services encounters (care contacts) from MHSDS

Clinical Purpose:
- Establishing use of mental health services
- Understanding patient service preference
- Care coordination management across providers

Includes ALL persons (active, inactive, deceased) within 5 years following intermediate layer principles.
only includes care contacts that were attended

*/

select 
    c.uniq_care_cont_id as encounter_id
    , b.sk_patient_id
    , c.care_cont_date as start_date
    , clin_cont_dur_of_care_cont as duration
    -- Use average cost per day for mental health care contact according to National Cost Collection (£302 average cost per day, adjusted by 15.7% uplift for NCL)
    -- Source: https://www.england.nhs.uk/costing-in-the-nhs/national-cost-collection/
    , 302 * 1.157 as proxy_cost
    , 'MHSDS' as source
from 
    {{ ref('stg_mhsds_carecontact')}} as c 
inner join 
    {{ ref('stg_mhsds_activesubmission')}} as a
    on c.uniq_submission_id = a.uniq_submission_id
left join 
    {{ ref('stg_mhsds_bridging')}} as b
    on c.person_id = b.person_id 
