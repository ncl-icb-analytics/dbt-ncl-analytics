{% macro get_ltc_lcs_medication_statements_latest(valuesets) %}

{%- if valuesets is none -%}
    {{ exceptions.raise_compiler_error("get_ltc_lcs_medication_statements_latest requires valuesets parameter") }}
{%- endif -%}

{# Convert comma-separated values to quoted list if needed #}
{%- if "'" not in valuesets -%}
    {%- set valuesets = "'" ~ valuesets.replace(',', "','") ~ "'" -%}
{%- endif -%}

select
    ms.id as medication_statement_id,
    ms.patient_id,
    pp.person_id,
    pp.sk_patient_id,
    ms.clinical_effective_date as statement_date,
    ms.medication_name as statement_medication_name,
    ms.dose as statement_dose,
    ms.quantity_value as statement_quantity_value,
    ms.quantity_unit as statement_quantity_unit,
    ms.date_recorded,
    ms.authorisation_type_code,
    ms.authorisation_type_display,
    ms.issue_method,
    ms.is_active,
    ms.cancellation_date,
    ms.expiry_date,
    ms.age_at_event,
    ms.mapped_concept_code as concept_code,
    ms.mapped_concept_display as concept_display,
    ec.valueset_id,
    vs.valueset_friendly_name,
    bnf.bnf_code,
    bnf.bnf_name
from {{ ref('stg_olids_medication_statement') }} as ms
inner join {{ ref('stg_reference_ltc_lcs_expanded_concepts') }} as ec
    on ms.mapped_concept_code = ec.snomed_code
left join {{ ref('stg_reference_ltc_lcs_valuesets') }} as vs
    on ec.valueset_id = vs.valueset_id
join {{ ref('int_patient_person_unique') }} as pp
    on ms.patient_id = pp.patient_id
left join {{ ref('stg_reference_bnf_latest') }} as bnf
    on ms.mapped_concept_code = bnf.snomed_code
where ms.clinical_effective_date is not null
    and (
        ec.valueset_id in ({{ valuesets }})
        or upper(vs.valueset_friendly_name) in (upper({{ valuesets }}))
    )
qualify row_number() over (partition by pp.person_id, ec.valueset_id order by ms.clinical_effective_date desc, ms.id desc) = 1

{% endmacro %}
