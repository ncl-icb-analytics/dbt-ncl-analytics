{% macro get_ltc_lcs_medication_orders_latest(valuesets) %}

{%- if valuesets is none -%}
    {{ exceptions.raise_compiler_error("get_ltc_lcs_medication_orders_latest requires valuesets parameter") }}
{%- endif -%}

{# Convert comma-separated values to quoted list if needed #}
{%- if "'" not in valuesets -%}
    {%- set valuesets = "'" ~ valuesets.replace(',', "','") ~ "'" -%}
{%- endif -%}

select
    mo.id as medication_order_id,
    mo.medication_statement_id,
    mo.patient_id,
    pp.person_id,
    pp.sk_patient_id,
    mo.clinical_effective_date as order_date,
    mo.medication_name as order_medication_name,
    mo.dose as order_dose,
    mo.quantity_value as order_quantity_value,
    mo.quantity_unit as order_quantity_unit,
    mo.duration_days as order_duration_days,
    mo.estimated_cost,
    mo.statement_medication_name,
    mo.mapped_concept_code as concept_code,
    mo.mapped_concept_display as concept_display,
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
where mo.clinical_effective_date is not null
    and (
        ec.valueset_id in ({{ valuesets }})
        or upper(vs.valueset_friendly_name) in (upper({{ valuesets }}))
    )
qualify row_number() over (partition by pp.person_id, ec.valueset_id order by mo.clinical_effective_date desc, mo.id desc) = 1

{% endmacro %}
