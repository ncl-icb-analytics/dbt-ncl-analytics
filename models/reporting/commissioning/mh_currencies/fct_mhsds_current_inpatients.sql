/*
People currently occupying a mental health bed.

A spell counts as current when int_mhsds_spell_encounters classifies it as
'open': no discharge date, and the spell appears in an active submission
within 2 reporting periods of the latest — the strongest available evidence
of continuing occupancy. Orphaned undischarged spells (stopped being
submitted) are excluded.

One person can hold more than one open spell (transfers, provider data
overlaps); the grain is the spell, not the person.
*/

select
    c.uniq_hosp_prov_spell_num
    , c.uniq_serv_req_id
    , c.person_id
    , c.sk_patient_id
    , c.org_id_prov
    , org.organisation_name as provider_name
    , c.start_date_hosp_prov_spell as admission_date
    , datediff(day, c.start_date_hosp_prov_spell, current_date) as days_in_bed
    , datediff(month, c.start_date_hosp_prov_spell, current_date) as months_in_bed
    , c.age_hosp_start_date as age_at_admission
    , c.is_cyp
    , c.mh_admitted_patient_class
    , c.setting_code
    , c.setting_name
    , c.icd10_3
    , c.diagnosis_category
    , c.winning_tier
    , c.currency_group
    , c.currency_code
from {{ ref('int_mhsds_spell_currency') }} as c
left join {{ ref('stg_dictionary_dbo_organisation') }} as org
    on c.org_id_prov = org.organisation_code
where c.end_date_source = 'open'
