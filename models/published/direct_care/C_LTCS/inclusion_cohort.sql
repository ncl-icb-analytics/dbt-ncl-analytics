{{
    config(
        materialized='table')
}}


/*
Inclusion criteria cohort for CLTCS

Clinical Purpose:
- Supporting the C-LTCS product by identifying patients with relevant conditions and recent outpatient activity

Testing:
- Actual table will use cambridge multimorbidity logic and filter to relevant appointments once finalised

*/
with conditions_inclusion as (
    select distinct person_id, 1 as has_condition
    from {{ ref('dim_person_conditions')}}
    where cardiovascular_conditions + respiratory_conditions + metabolic_conditions > 3
),
op_inclusion as(
    select distinct sk_patient_id, 1 as has_recent_op
    from {{ ref('fct_person_sus_op_recent')}}
    where op_att_tot_12mo > 2
),
-- sk_patient_multimapping_GP_reg as ( -- do we really want to handle like this or warn?
--     SELECT
--     -- Core Identifiers
--     hist.person_id,
--     hist.sk_patient_id,

--     hist.birth_year,
--     hist.birth_date_approx,
--     hist.birth_date_approx_end_of_month,
--     hist.death_year,
--     hist.death_date_approx,

--     -- Practice Registration (current or latest)
--     hist.practice_code,
--     hist.practice_name,
--     hist.registration_start_date,
--     hist.registration_end_date,

-- FROM {{ ref('dim_person_demographics_historical') }} hist

-- WHERE hist.is_current = TRUE and 
-- qualify row_number() over (
--     partition by sk_patient_id, birth_date_approx
--     order by registration_start_date desc-- THIS NEEDS CHECKING
-- ) = 1
-- )
-- ,
potentially_fragmented_sk_patient_ids as (
    SELECT 
    p.sk_patient_id,
    COUNT(DISTINCT pp.person_id) as person_count,
    ARRAY_AGG(DISTINCT pp.person_id) as person_ids
    FROM {{ source('olids', 'PATIENT') }} p
    JOIN {{ source('olids', 'PATIENT_PERSON') }} pp ON p.id = pp.patient_id
    GROUP BY p.sk_patient_id
    HAVING COUNT(DISTINCT pp.person_id) > 1
    ORDER BY person_count DESC
),
potentially_fragmented_person_ids as (
    SELECT 
        pp.person_id,
        COUNT(DISTINCT p.sk_patient_id) as patient_count,
        ARRAY_AGG(DISTINCT p.sk_patient_id) as sk_patient_ids
    FROM {{ source('olids', 'PATIENT') }} p
    JOIN {{ source('olids', 'PATIENT_PERSON') }} pp ON p.id = pp.patient_id
    GROUP BY pp.person_id
    HAVING COUNT(DISTINCT p.sk_patient_id) > 1
    ORDER BY patient_count DESC
)
select pp.sk_patient_id as patient_id
    ,pp.hx_flake as re_id_key
    ,pp.person_id as olids_id
    ,pd.practice_code
    ,pd.practice_name
    ,pd.pcn_code
    ,pd.pcn_name
    ,pd.main_language
    ,pd.age
    ,pd.gender
    ,pd.ethnicity_category
    ,case when pc.has_condition = 1 or op.has_recent_op = 1 then 1 else 0 end as eligible
    ,case when pp.sk_patient_id in (select sk_patient_id from potentially_fragmented_sk_patient_ids) then 1 else 0 end as fragmented_sk_patient_id_flag -- poor mapping of multiple person_ids to one sk_patient_id
    ,case when pp.person_id in (select person_id from potentially_fragmented_person_ids) then 1 else 0 end as fragmented_person_id_flag -- poor mapping of multiple sk_patient_ids to one person_id
from {{ref('dim_person_pseudo')}} pp
left join conditions_inclusion pc
    on pc.person_id = pp.person_id
left join op_inclusion op
    on op.sk_patient_id = pp.sk_patient_id
left join {{ref('dim_person_demographics')}} pd
    on pd.person_id = pp.person_id
where pd.is_deceased = FALSE 
    and pcn_code in (select distinct pcn_code from {{source('c_ltcs','MDT_LOOKUP')}} )
  --  and fragmented_sk_patient_id_flag = 0 -- keep warning for awareness of person/patient mapping issues
-- and fragmented_person_id_flag = 0