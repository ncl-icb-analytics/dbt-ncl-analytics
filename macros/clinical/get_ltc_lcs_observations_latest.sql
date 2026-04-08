{% macro get_ltc_lcs_observations_latest(valuesets) %}

{%- if valuesets is none -%}
    {{ exceptions.raise_compiler_error("get_ltc_lcs_observations_latest requires valuesets parameter") }}
{%- endif -%}

{# Normalise valuesets into a clean token list, then render quoted forms #}
{%- set valueset_tokens = [] -%}
{%- for raw in valuesets.replace("'", "").split(",") -%}
    {%- set token = raw | trim -%}
    {%- if token -%}{%- do valueset_tokens.append(token) -%}{%- endif -%}
{%- endfor -%}
{%- if valueset_tokens | length == 0 -%}
    {{ exceptions.raise_compiler_error("get_ltc_lcs_observations_latest requires non-empty valuesets parameter") }}
{%- endif -%}
{%- set valuesets_quoted = "'" ~ valueset_tokens | join("','") ~ "'" -%}
{%- set valuesets_upper_quoted = "'" ~ (valueset_tokens | map('upper') | join("','")) ~ "'" -%}

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
    o.episodicity_concept_id,
    ec.valueset_id,
    vs.valueset_friendly_name
from {{ ref('stg_olids_observation') }} as o
inner join {{ ref('stg_reference_ltc_lcs_expanded_concepts') }} as ec
    on o.mapped_concept_code = ec.snomed_code
left join {{ ref('stg_reference_ltc_lcs_valuesets') }} as vs
    on ec.valueset_id = vs.valueset_id
where (
    ec.valueset_id in ({{ valuesets_quoted }})
    or upper(vs.valueset_friendly_name) in ({{ valuesets_upper_quoted }})
)
qualify row_number() over (partition by o.person_id, ec.valueset_id order by o.clinical_effective_date desc, o.id desc) = 1

{% endmacro %}
