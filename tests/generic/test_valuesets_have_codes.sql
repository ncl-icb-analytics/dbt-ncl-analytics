{% test valuesets_have_codes(model, valuesets) %}

-- Generic test to verify that all referenced LTC LCS valuesets exist and contain codes.
-- Ensures that risk stratification models won't silently fail due to empty/missing valuesets.
--
-- Usage in yml:
--   tests:
--     - valuesets_have_codes:
--         valuesets: ['on_dm_reg_pg1_hrc_vs1', 'on_dm_reg_pg1_hrc_vs2']

with valueset_codes as (
    select
        ec.valueset_id,
        coalesce(vs.valueset_friendly_name, ec.valueset_id) as valueset_name,
        count(distinct ec.snomed_code) as code_count
    from {{ ref('stg_reference_ltc_lcs_expanded_concepts') }} as ec
    left join {{ ref('stg_reference_ltc_lcs_valuesets') }} as vs
        on ec.valueset_id = vs.valueset_id
    where ec.valueset_id in (
        {% for vs in valuesets %}
            '{{ vs }}'{% if not loop.last %}, {% endif %}
        {% endfor %}
    )
    or upper(vs.valueset_friendly_name) in (
        {% for vs in valuesets %}
            upper('{{ vs }}'){% if not loop.last %}, {% endif %}
        {% endfor %}
    )
    group by ec.valueset_id, vs.valueset_friendly_name
),

expected_valuesets as (
    {% for vs in valuesets %}
        select '{{ vs }}' as expected_valueset{% if not loop.last %} union all {% endif %}
    {% endfor %}
),

validation as (
    select
        e.expected_valueset,
        vc.valueset_name,
        coalesce(vc.code_count, 0) as code_count,
        case
            when vc.valueset_id is null then 'Valueset not found'
            when vc.code_count = 0 then 'Valueset has no codes'
            else null
        end as error_reason
    from expected_valuesets e
    left join valueset_codes vc
        on e.expected_valueset = vc.valueset_id
        or upper(e.expected_valueset) = upper(vc.valueset_name)
)

-- Return rows where valueset is missing or empty (test fails if any rows returned)
select
    expected_valueset,
    valueset_name,
    code_count,
    error_reason
from validation
where error_reason is not null

{% endtest %}
