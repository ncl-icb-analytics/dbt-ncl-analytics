{% macro get_ltc_lcs_observations(valuesets) %}

{%- if valuesets is none -%}
    {{ exceptions.raise_compiler_error("get_ltc_lcs_observations requires valuesets parameter") }}
{%- endif -%}

{# Convert comma-separated values to quoted list if needed #}
{%- if "'" not in valuesets -%}
    {%- set valuesets = "'" ~ valuesets.replace(',', "','") ~ "'" -%}
{%- endif -%}

select
    o.id as observation_id,
    o.patient_id,
    o.person_id,
    o.clinical_effective_date,
    o.result_value,
    o.result_value_units_concept_id,
    o.result_unit_display,
    o.result_text,
    o.is_problem,
    o.is_review,
    o.problem_end_date,
    o.mapped_concept_id as concept_id,
    o.mapped_concept_code as concept_code,
    o.mapped_concept_display as concept_display,
    ec.valueset_id,
    vs.valueset_friendly_name
from {{ ref('stg_olids_observation') }} as o
inner join {{ ref('stg_reference_ltc_lcs_expanded_concepts') }} as ec
    on o.mapped_concept_code = ec.snomed_code
left join {{ ref('stg_reference_ltc_lcs_valuesets') }} as vs
    on ec.valueset_id = vs.valueset_id
where (
    ec.valueset_id in ({{ valuesets }})
    or upper(vs.valueset_friendly_name) in (upper({{ valuesets }}))
)
qualify row_number() over (partition by o.id, ec.valueset_id order by ec.snomed_code) = 1

{% endmacro %}
