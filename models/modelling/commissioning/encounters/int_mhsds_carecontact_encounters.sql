/*
Mental Health Services encounters (care contacts) from MHSDS

Clinical Purpose:
- Establishing use of mental health services
- Understanding patient service preference
- Care coordination management across providers

Includes ALL persons (active, inactive, deceased) within 5 years following intermediate layer principles.
Includes any attendance status in MHSDS care contacts.
There maybe >1 encounters on the same date for a patient.

*/

select 
    c.uniq_care_cont_id as encounter_id
    , b.sk_patient_id
    , c.org_id_prov
    , c.attend_or_dna_code
    ,CASE 
    WHEN c.attend_or_dna_code in ('5','6') THEN 'Attended' 
    WHEN c.attend_or_dna_code in ('3','7') THEN 'DNA/Late' 
    WHEN c.attend_or_dna_code in ('2') THEN 'Cancelled by Patient' 
    WHEN c.attend_or_dna_code in ('4') THEN 'Cancelled by Provider'
    ELSE 'Unknown' END AS attendance_status
    ,org.organisation_name as provider_name
    ,CASE 
    WHEN c.ORG_ID_PROV = 'G6V2S' THEN 'NLFT'
    WHEN c.ORG_ID_PROV = 'TAF' THEN 'C&I'
    WHEN c.ORG_ID_PROV = 'RNK' THEN 'T&P'
    WHEN c.ORG_ID_PROV = 'RRP' THEN 'BEH'
    WHEN c.ORG_ID_PROV = 'RAT' THEN 'NELFT'
    WHEN c.ORG_ID_PROV = 'RWK' THEN 'ELFT'
    WHEN c.ORG_ID_PROV = 'RAL' THEN 'RFL'
    WHEN c.ORG_ID_PROV = 'RKE' THEN 'WHIT'
    WHEN c.ORG_ID_PROV = 'RKL' THEN 'WLT'
    WHEN c.ORG_ID_PROV = 'RV5' THEN 'SLAM'
    WHEN c.ORG_ID_PROV = 'RV3' THEN 'CNWL'
    WHEN c.ORG_ID_PROV = 'RQY' THEN 'SWLSTG'
    ELSE 'Other' END as provider_short_name
    , c.care_cont_date as start_date
    , clin_cont_dur_of_care_cont as duration
    , dm_icb_commissioner
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
    {{ ref('stg_dictionary_dbo_organisation')}} as org
     on c.ORG_ID_PROV = org.ORGANISATION_CODE
left join 
    {{ ref('stg_mhsds_bridging')}} as b
    on c.person_id = b.person_id 
