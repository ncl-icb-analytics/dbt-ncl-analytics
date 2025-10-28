/*
Mental Health Services encounters (spells) from MHSDS

Clinical Purpose:
- Establishing use of mental health inpatient services
- Understanding patient service preference
- Care coordination management across providers

Includes ALL persons (active, inactive, deceased) within 5 years following intermediate layer principles.
only includes care contacts that were attended

*/

select 
    c.uniq_hosp_prov_spell_num as encounter_id
    , b.sk_patient_id
    , c.start_date_hosp_prov_spell as start_date
    , c.source_adm_mh_hosp_prov_spell as admission_source_code
    , s.source_of_admission_name as admission_source
    , c.meth_adm_mh_hosp_prov_spell as admission_method_code
    , m.admission_method_name as admission_method
    , coalesce(c.disch_date_hosp_prov_spell, c.estimated_disch_date_hosp_prov_spell) as end_date
    , datediff(day, c.start_date_hosp_prov_spell, coalesce(c.disch_date_hosp_prov_spell, c.estimated_disch_date_hosp_prov_spell, current_date)) as duration_to_date
    , datediff(day, c.start_date_hosp_prov_spell, coalesce(c.disch_date_hosp_prov_spell, c.estimated_disch_date_hosp_prov_spell, current_date))
        -- Use average cost per day for mental health spell according to National Cost Collection (£651 average cost per day, adjusted by +15.7% for NCL)
        -- Source: https://www.england.nhs.uk/costing-in-the-nhs/national-cost-collection/
        * 651 * 1.157 as proxy_cost
    , 'MHSDS' as source
from 
    {{ ref('stg_mhsds_spell')}} as c 
inner join 
    {{ ref('stg_mhsds_activesubmission')}} as a
    on c.uniq_submission_id = a.uniq_submission_id
left join 
    {{ ref('stg_mhsds_bridging')}} as b
    on c.person_id = b.person_id 
left join 
    {{ ref('stg_dictionary_ip_sourceofadmissions')}} as s
    on c.source_adm_mh_hosp_prov_spell = s.bk_source_of_admission_code
left join 
    {{ ref('stg_dictionary_ip_admissionmethods')}} as m
    on c.meth_adm_mh_hosp_prov_spell = m.bk_admission_method_code
