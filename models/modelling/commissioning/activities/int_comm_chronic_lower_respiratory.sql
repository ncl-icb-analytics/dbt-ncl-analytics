-- SET VARIABLES
-- Only asthma and copd related codes << 195967001 |asthma| or << 13645005 |COPD|
{% 
    set snomed_diagnosis_list_resp = dbt_utils.get_column_values(
        ref('stg_reference_combined_codesets'),
        'code',
        where="cluster_id = 'LOWER_RESP_COND'"
    ) 
%}

{% 
    set snomed_diagnosis_list_all = dbt_utils.get_column_values(
        ref('stg_reference_combined_codesets'),
        'code',
        where="cluster_id = 'TOT_RESP_COND'"
    ) 
%}

{% set snomed_diagnosis_exclude = snomed_diagnosis_list_all | reject('in', snomed_diagnosis_list_resp) | list %}

{% 
    set respiratory_encounter_list = dbt_utils.get_column_values(
        ref('int_comm_respiratory') ,
        'visit_occurrence_id',
    ) 
%}

-- specific icd10 codes (lower acute, lower chronic)
{% set icd10_prefix_list_inclusion = ['J4', 'J2'] %}
{% set icd10_prefix_list_exclusion = ['J0', 'J1', 'J3', 'J5', 'J6', 'J7', 'J8', 'J9'] %}

-- ICD-10 codes for respiratory diseases:
-- Acute upper respiratory infections (J00-J06)
-- Influenza and pneumonia (J09-J18)
-- Other acute lower respiratory infections (J20-J22) -- INCLUDED
-- Other diseases of upper respiratory tract (J30-J39)
-- Chronic lower respiratory diseases (J40-J4A) -- INCLUDED
-- Lung diseases due to external agents (J60-J70)
-- Other respiratory diseases principally affecting the interstitium (J80-J84)
-- Suppurative and necrotic conditions of the lower respiratory tract (J85-J86)
-- Other diseases of the pleura (J90-J94)
-- Intraoperative and postprocedural complications and disorders of respiratory system, not elsewhere classified (J95)
-- Other diseases of the respiratory system (J96-J99)

-- all adult and respiratory hrg codes
{% set hrg_prefix_list_inclusion = ['DZ15', 'DZ22', 'DZ23', 'DZ65'] %}
{% set hrg_prefix_list_exclusion = ['DZ01','DZ02','DZ09','DZ10','DZ11','DZ12','DZ14','DZ16','DZ17','DZ18','DZ19','DZ20','DZ24','DZ25','DZ26','DZ27','DZ28','DZ29','DZ30','DZ31','DZ32','DZ33','DZ36','DZ37','DZ38','DZ42','DZ45','DZ46','DZ49','DZ50','DZ51','DZ52','DZ55','DZ56','DZ57','DZ58','DZ59','DZ60','DZ62','DZ63','DZ64','DZ66','DZ67','DZ68','DZ69','DZ70','DZ71'] %}

-- ASTHMA = DZ15, ACUTE LOWER INFECTION = DZ22, BROCHOPNEUMONIA = DZ23
-- specific procedures
{% set OPCS4_prefix_list = ['E49', 'E63.4', 'E65', 'E25', 'E36'] %}

with ae_attendance_excluded as (
    select visit_occurrence_id
    from {{ ref('int_sus_ae_encounters') }}
    where (
        -- Must be in respiratory list
        visit_occurrence_id in {{ to_sql_list(respiratory_encounter_list) }}
    ) 
    and (
        -- Meets exclusion criteria
        (
            chief_complaint_code in ('13094009', '70407001', '262599003')
            or 
            {% for prefix in icd10_prefix_list_exclusion %}
                startswith(primary_diagnosis_code_icd10, '{{ prefix }}')
                {% if not loop.last %} or {% endif %}
            {% endfor %}
            or 
            primary_diagnosis_code_snomed in {{ to_sql_list(snomed_diagnosis_exclude) }}
        )
        -- BUT does NOT meet inclusion criteria (exclusion override)
        and not (
            -- Inclusion criteria checks
            {% for prefix in icd10_prefix_list_inclusion %}
                startswith(primary_diagnosis_code_icd10, '{{ prefix }}')
                {% if not loop.last %} or {% endif %}
            {% endfor %}
            or
            primary_diagnosis_code_snomed in {{ to_sql_list(snomed_diagnosis_list_resp) }}
            or
            {% for prefix in hrg_prefix_list_inclusion %}
                startswith(hrg_code, '{{ prefix }}')
                {% if not loop.last %} or {% endif %}
            {% endfor %}
        )
    )
),

ae_diagnosis_excluded as ( 
    select visit_occurrence_id
    from {{ ref('int_sus_ae_diagnosis') }} 
    where (
        -- is in respiratory list
        visit_occurrence_id in {{ to_sql_list(respiratory_encounter_list) }})
        -- in exclusion list 
        and (
            {% for prefix in icd10_prefix_list_exclusion %}
                startswith(mapped_icd10_code, '{{ prefix }}')
                {% if not loop.last %} or {% endif %}
            {% endfor %}
            or 
            source_concept_code in {{ to_sql_list(snomed_diagnosis_exclude) }}
            )
        and not (
            -- not in inclusion list 
            {% for prefix in icd10_prefix_list_inclusion %}
                startswith(mapped_icd10_code, '{{ prefix }}')
                {% if not loop.last %} or {% endif %}
            {% endfor %} 
        or 
        source_concept_code in {{ to_sql_list(snomed_diagnosis_list_resp) }}
        )
),

-- consider adding procedures


-- Admitted
admitted_spells_excluded as (
    select visit_occurrence_id
    from {{ ref('int_sus_ip_encounters') }}
    where (
        -- is in respiratory list
        visit_occurrence_id in {{ to_sql_list(respiratory_encounter_list) }})
        -- in exclusion list 
        and (
            {% for prefix in icd10_prefix_list_exclusion %}
                startswith(primary_diagnosis_code, '{{ prefix }}')
                {% if not loop.last %} or {% endif %}
            {% endfor %}
        
      or LEFT(hrg_code, 4) IN {{ to_sql_list(hrg_prefix_list_exclusion) }} )-- consider switching to core_hrg_chapter = D if want more broad
    -- is not in inclusion list 
    and not (
        {% for prefix in icd10_prefix_list_inclusion %}
            startswith(primary_diagnosis_code, '{{ prefix }}')
            {% if not loop.last %} or {% endif %}
        {% endfor %}
        or 
        LEFT(hrg_code, 4) IN {{ to_sql_list(hrg_prefix_list_inclusion) }}
    )
),


admitted_procedure_hrg_excluded as (
    select visit_occurrence_id
    from {{ ref('int_sus_apc_procedure_hrg') }}
    where 
    -- is in respiratory list
        visit_occurrence_id in {{ to_sql_list(respiratory_encounter_list) }}
        -- in exclusion list 
        and (LEFT(source_concept_code, 4) IN {{ to_sql_list(hrg_prefix_list_exclusion ) }})
        -- is not in inclusion list 
        and not (LEFT(source_concept_code, 4) IN {{ to_sql_list(hrg_prefix_list_inclusion) }})
),


-- Outpatient
-- Notable exclusion: respiratory infections (J1*, J2*, B97.4)
outpatient_appts_summary_excluded as (
    select visit_occurrence_id
    from {{ ref('int_sus_op_appointments') }}
    where (
        -- is in respiratory list
        visit_occurrence_id in {{ to_sql_list(respiratory_encounter_list) }}) 
        -- in exclusion list 
        and (
            {% for prefix in icd10_prefix_list_exclusion %}
                startswith(primary_diagnosis_code, '{{ prefix }}')
                {% if not loop.last %} or {% endif %}
            {% endfor %} 
    or LEFT(core_hrg_code, 4) IN {{ to_sql_list(hrg_prefix_list_exclusion) }})
    -- and are not in inclusion list 
    and not (
        {% for prefix in icd10_prefix_list_inclusion %}
            startswith(primary_diagnosis_code, '{{ prefix }}')
            {% if not loop.last %} or {% endif %}
        {% endfor %}
        or 
        LEFT(core_hrg_code, 4) IN {{ to_sql_list(hrg_prefix_list_inclusion) }}
    )
),

outpatient_diagnosis_excluded as ( -- keeping as seems less catch all than APC
    select visit_occurrence_id
    from {{ ref('int_sus_op_diagnosis') }}
    where (
        -- is in respiratory list
        visit_occurrence_id in {{ to_sql_list(respiratory_encounter_list) }}) 
        -- in exclusion list 
        and (
            {% for prefix in icd10_prefix_list_exclusion %}
                startswith(source_concept_code, '{{ prefix }}')
                {% if not loop.last %} or {% endif %}
            {% endfor %}
        ) 
        -- and is not in inclusion list 
        and not (
            {% for prefix in icd10_prefix_list_inclusion %}
                startswith(source_concept_code, '{{ prefix }}')
                {% if not loop.last %} or {% endif %}
            {% endfor %}
        )
),

-- DZ = Adult Respiratory System Procedures and Disorder
-- PD = paediatric respiratory system procedures and disorder


exclusion_list as (
    select visit_occurrence_id
    from ae_attendance_excluded
    
    union all

    select visit_occurrence_id
    from ae_diagnosis_excluded
    
    union all
    
    select visit_occurrence_id
    from admitted_spells_excluded
    
    union all
    
    select visit_occurrence_id
    from admitted_procedure_hrg_excluded

    union all
    
    select visit_occurrence_id
    from outpatient_appts_summary_excluded
    
    union all
    
    select visit_occurrence_id
    from outpatient_diagnosis_excluded

)

select distinct 
    resp.visit_occurrence_id, 
    true as lower_respiratory_encounter
from {{ref('int_comm_respiratory') }} resp
where resp.visit_occurrence_id not in (select visit_occurrence_id from exclusion_list)