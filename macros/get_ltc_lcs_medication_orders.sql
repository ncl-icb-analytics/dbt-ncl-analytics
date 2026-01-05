{% macro get_ltc_lcs_medication_orders(valueset_ids=none, valueset_friendly_names=none) %}

{%- if valueset_ids is none and valueset_friendly_names is none -%}
    {{ exceptions.raise_compiler_error("get_ltc_lcs_medication_orders requires either valueset_ids or valueset_friendly_names parameter") }}
{%- endif -%}

select
    mo.medication_order_id,
    mo.medication_statement_id,
    mo.patient_id,
    pp.person_id,
    pp.sk_patient_id,
    mo.order_date,
    mo.order_medication_name,
    mo.order_dose,
    mo.order_quantity_value,
    mo.order_quantity_unit,
    mo.order_duration_days,
    mo.estimated_cost,
    mo.statement_medication_name,
    mo.mapped_concept_code,
    mo.mapped_concept_display,
    ec.valueset_id,
    vs.valueset_friendly_name,
    bnf.bnf_code,
    bnf.bnf_name
from {{ ref('stg_olids_medication_order') }} as mo
inner join {{ ref('stg_reference_ltc_lcs_expanded_concepts') }} as ec
    on mo.mapped_concept_code = ec.snomed_code
left join {{ ref('stg_reference_ltc_lcs_valuesets') }} as vs
    on ec.valueset_id = vs.valueset_id
join {{ ref('int_patient_person_unique') }} as pp
    on mo.patient_id = pp.patient_id
left join {{ ref('stg_reference_bnf_latest') }} as bnf
    on mo.mapped_concept_code = bnf.snomed_code
where mo.order_date is not null
{%- if valueset_ids is not none %}
    and ec.valueset_id in ({{ valueset_ids }})
{%- endif %}
{%- if valueset_friendly_names is not none %}
    and vs.valueset_friendly_name in ({{ valueset_friendly_names }})
{%- endif %}
qualify row_number() over (partition by mo.medication_order_id, ec.valueset_id order by ec.snomed_code) = 1

{% endmacro %}
