with olids_carehome as (
    SELECT
        person_id,
        residence_type as care_home_type_olids,
    FROM {{ref('dim_person_care_home')}}  
    where CONCEPT_CODE <> '1092571000000100' -- ignore employed in nursing home
),

acorn_carehome as (
    select sk_patient_id,
        der_care_home_service_type as care_home_type_acorn
    from {{ ref('int_acorn_care_home') }}
    ),

bridging_table as (
    select sk_patient_id, person_id 
    from {{ref('dim_person_pseudo')}} 
    )


select bt.person_id
    , bt.sk_patient_id
    , coalesce(oc.care_home_type_olids, ac.care_home_type_acorn) as care_home_type
    , case 
        when oc.care_home_type_olids is not null and ac.care_home_type_acorn is not null then 'BOTH'
        when oc.care_home_type_olids is not null then 'OLIDS' 
        when ac.care_home_type_acorn is not null then 'ACORN' 
        else null end as care_home_source
from bridging_table bt
left join olids_carehome oc on bt.person_id = oc.person_id
left join acorn_carehome ac on bt.sk_patient_id = ac.sk_patient_id
where oc.care_home_type_olids is not null or ac.care_home_type_acorn is not null