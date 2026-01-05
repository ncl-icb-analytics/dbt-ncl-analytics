{% macro get_ltc_lcs_observations(valueset_ids=none, valueset_friendly_names=none) %}

{%- if valueset_ids is none and valueset_friendly_names is none -%}
    {{ exceptions.raise_compiler_error("get_ltc_lcs_observations requires either valueset_ids or valueset_friendly_names parameter") }}
{%- endif -%}

select
    o.id,
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
    o.mapped_concept_id,
    o.mapped_concept_code,
    o.mapped_concept_display,
    ec.valueset_id,
    vs.valueset_friendly_name
from {{ ref('stg_olids_observation') }} as o
inner join {{ ref('stg_reference_ltc_lcs_expanded_concepts') }} as ec
    on o.mapped_concept_code = ec.snomed_code
left join {{ ref('stg_reference_ltc_lcs_valuesets') }} as vs
    on ec.valueset_id = vs.valueset_id
where 1=1
{%- if valueset_ids is not none %}
    and ec.valueset_id in ({{ valueset_ids }})
{%- endif %}
{%- if valueset_friendly_names is not none %}
    and vs.valueset_friendly_name in ({{ valueset_friendly_names }})
{%- endif %}
qualify row_number() over (partition by o.id, ec.valueset_id order by ec.snomed_code) = 1

{% endmacro %}
