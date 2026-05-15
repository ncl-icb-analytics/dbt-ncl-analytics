{{
    config(
        materialized='table')
}}


/*
Patient data processing for CLTCS

Clinical Purpose:
- All coded GP observations for patients in the C-LTCS cohort over a rolling
  1 year window. Minimal filtering by design so downstream consumers can
  decide how to summarise (e.g. by concept, episodicity, problem flag).

Source choice:
- stg_olids_observation is used directly because the intermediate observation
  layer (int_*_all) is concept-specific (HbA1c, BP, eGFR, etc.) and would
  drop any observation not covered by a curated cluster. For an "all
  observations" feed the staging layer is the correct grain.
*/

{% set observation_cutoff = -6 %}

with inclusion_list as (
    select patient_id, area_code, olids_id
    from {{ ref('cltcs_patient_list')}}
),

gp_observations as (
    select
        il.patient_id,
        il.area_code,
        il.olids_id,
        o.id as observation_id,
        -- Fall back to date_recorded when clinical_effective_date is in the
        -- future relative to when the record was written (matches the data
        -- quality fix applied by the get_observations macro).
        case
            when o.clinical_effective_date > o.date_recorded then o.date_recorded
            else o.clinical_effective_date
        end as clinical_effective_date,
        o.mapped_concept_code,
        o.mapped_concept_display,
        o.result_value,
        o.result_text,
        o.result_unit_display,
        o.is_problem,
        coalesce(epi_target_concept.display, o.episodicity_concept_id) as episodicity_display,
        p.last_name as practitioner_last_name,
        p.first_name as practitioner_first_name,
        p.title as practitioner_title
    from {{ ref('stg_olids_observation') }} o
    inner join inclusion_list il
        on il.olids_id = o.person_id
    left join {{ ref('stg_olids_practitioner') }} p
        on o.practitioner_id = p.id
    {{ join_concept_display('o.episodicity_concept_id', 'epi_', is_primary_only=true) }}
    where o.clinical_effective_date is not null
        and o.clinical_effective_date between dateadd(month, {{ observation_cutoff }}, current_date()) and current_date()
)

select
    patient_id,
    area_code,
    olids_id,
    observation_id,
    clinical_effective_date,
    mapped_concept_code,
    mapped_concept_display,
    result_value,
    result_text,
    result_unit_display,
    is_problem,
    episodicity_display,
    practitioner_title,
    practitioner_first_name,
    practitioner_last_name
from gp_observations
