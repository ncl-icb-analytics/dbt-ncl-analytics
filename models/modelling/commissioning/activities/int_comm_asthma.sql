with asthma_drug_codes as (
    SELECT DISTINCT 
            code as mapped_concept_code,
            cluster_id,
            cluster_description,
            code_description
        FROM {{ ref('stg_reference_combined_codesets') }}
        WHERE UPPER(cluster_id) = 'ASTTRT_COD'
    ),

specialty_filters as (
    -- Combine filter conditions to avoid repetition
    select distinct visit_occurrence_id
    from {{ ref('int_sus_ae_encounters')}}
    WHERE (
        (chief_complaint_ecds_group1 like 'Airway / breathing' and 
        chief_complaint_code not in ('13094009', -- aponea in newborns
                                    '70407001', -- stridor
                                    '262599003'))
        OR LEFT(core_hrg_code, 2) IN ('SB', 'SC')
        OR main_specialty_code IN ('800', '370')
        OR treatment_function_code IN ('800', '370'))
    --    OR PRIMARY_DIAG BETWEEN 'C00' AND 'C97'
    --    OR PRIMARY_DIAG BETWEEN 'D00' AND 'D09')
    
    union all

    select distinct visit_occurrence_id
    from {{ ref('int_sus_ae_diagnosis')}}
    where mapped_icd10_code like 'J4%'

    union all

    select distinct visit_occurrence_id
    from {{ ref('int_sus_ae_procedure')}}
    where mapped_icd10_code like 'J4%'
),

asthma_encounters as (
    select distinct visit_occurrence_id
    from specialty_filters
)

select 
    e.visit_occurrence_id
    , e.start_date
    , e.organisation_id
    , e.organisation_name
    , e.main_specialty_code
    , e.main_specialty_name
    , e.treatment_function_code
    , e.treatment_function_code_desc
    , e.core_hrg_code
    , e.core_hrg_desc
    , e.core_hrg_chapter_desc
from {{ ref('int_sus_op_encounters')}} e
inner join cancer_encounters m 
    on e.visit_occurrence_id = m.visit_occurrence_id