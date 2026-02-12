-- IN UEC
{% 
    set respiratory_code_list = dbt_utils.get_column_values(
        ref('stg_reference_combined_codesets'),
        'code',
        where="cluster_id = 'RESPIRATORY_CONDITIONS'"
    ) 
%}

with ae_chief_complaint as (
    select visit_occurrence_id
    from {{ ref('int_sus_ae_encounters') }}
    where chief_complaint_ecds_group1 = 'Airway / breathing' 
        and chief_complaint_code not in ('13094009', -- aponea in newborns
                                        '70407001', -- stridor
                                        '262599003')
),


ae_diagnosis as (
    select visit_occurrence_id
    from {{ ref('int_sus_ae_diagnosis') }} -- consider changing to all J?
    where mapped_icd10_code like 'J96.0%' -- acute respiratory failure
        or mapped_icd10_code like 'J96.1%' -- chronic respiratory failure
        or mapped_icd10_code like 'J4%' -- chronic lower respiratory disease
        {% if respiratory_code_list %}
        or source_concept_code in {{ to_sql_list(respiratory_code_list) }}
        {% endif %}
),

-- Admitted
-- Notable exclusion: respiratory infections (J1*, J2*, B97.4)
admitted_diagnosis as (
    select visit_occurrence_id
    from {{ ref('int_sus_ip_diagnosis') }}
    where concept_code like 'J96.0%' -- acute respiratory failure
        or concept_code like 'J96.1%' -- chronic respiratory failure
        or concept_code like 'J4%' -- chronic lower respiratory disease
),

-- DZ = Adult Respiratory System Procedures and Disorder
-- PD = paediatric respiratory system procedures and disorder
-- Notable exclusion: respiratory infections, check if need to exclude TB 
-- (DZ51Z Complex Tuberculosis with length of stay 29 days or more), 
-- DZ14 Pulmonary, Pleural or Other Tuberculosis
admitted_hrg as (
    select visit_occurrence_id
    from {{ ref('int_sus_apc_procedure_hrg') }}
    where LEFT(source_concept_code, 2) IN ('DZ', 'PD')
),

-- encounter main speciality code = 340? hrg core = D? otherwise too permissive?
admitted_procedure as (
    select visit_occurrence_id
    from {{ ref('int_sus_ip_procedure') }}
    where source_concept_code like 'E49%' -- diagnostic fiberoptic bronchoscopy
        or source_concept_code = 'E63.4' -- Endobronchial ultrasound- more of biopsy?
        or source_concept_code like 'E65%' -- nasendoscopy
        or source_concept_code like 'E25%' -- Diagnostic endoscopic examination of pharynx
        or source_concept_code like 'E36%' -- Diagnostic endoscopic examination of larynx
        -- Invasive ventilation with tracheostomy (E85.1) not included 
),

-- Outpatient
-- Notable exclusion: respiratory infections (J1*, J2*, B97.4)
outpatient_diagnosis as (
    select visit_occurrence_id
    from {{ ref('int_sus_op_diagnosis') }}
    where concept_code like 'J96.0%' -- acute respiratory failure
        or concept_code like 'J96.1%' -- chronic respiratory failure
        or concept_code like 'J4%' -- chronic lower respiratory disease
),

-- DZ = Adult Respiratory System Procedures and Disorder
-- PD = paediatric respiratory system procedures and disorder
outpatient_procedure_hrg as (
    select visit_occurrence_id
    from {{ ref('int_sus_op_procedure_hrg') }}
    where LEFT(source_concept_code, 2) IN ('DZ', 'PD')
),

outpatient_procedure as (
    select visit_occurrence_id
    from {{ ref('int_sus_op_procedure') }}
    where source_concept_code like 'E49%' -- diagnostic fiberoptic bronchoscopy
        or source_concept_code = 'E63.4' -- Endobronchial ultrasound- more of biopsy?
        or source_concept_code like 'E65%' -- nasendoscopy
        or source_concept_code like 'E25%' -- Diagnostic endoscopic examination of pharynx
        or source_concept_code like 'E36%' -- Diagnostic endoscopic examination of larynx
        -- Invasive ventilation with tracheostomy (E85.1) not included 
),

specialty_filters as (
    select visit_occurrence_id
    from ae_chief_complaint
    
    union all
    
    select visit_occurrence_id
    from ae_diagnosis
    
    union all
    
    select visit_occurrence_id
    from admitted_diagnosis
    
    union all
    
    select visit_occurrence_id
    from admitted_hrg
    
    union all
    
    select visit_occurrence_id
    from admitted_procedure
    
    union all
    
    select visit_occurrence_id
    from outpatient_diagnosis
    
    union all
    
    select visit_occurrence_id
    from outpatient_procedure_hrg
    
    union all
    
    select visit_occurrence_id
    from outpatient_procedure
)

select distinct 
    visit_occurrence_id, 
    true as respiratory_encounter
from specialty_filters