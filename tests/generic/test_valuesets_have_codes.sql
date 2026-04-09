{% test valuesets_have_codes(model, valuesets) %}

-- Generic test to verify that all referenced LTC LCS valuesets exist and contain codes
-- via at least one of the two paths used by get_ltc_lcs_observations[_latest]:
--   - mapped path: stg_reference_ltc_lcs_expanded_concepts (SNOMED expansions)
--   - source path: stg_reference_ltc_lcs_original_codes (raw EMIS codes)
-- A valueset passes if EITHER path has codes for it.
--
-- Usage in yml:
--   tests:
--     - valuesets_have_codes:
--         valuesets: ['on_dm_reg_pg1_hrc_vs1', 'on_dm_reg_pg1_hrc_vs2']

with mapped_codes as (
    select
        ec.valueset_id,
        count(distinct ec.snomed_code) as code_count
    from {{ ref('stg_reference_ltc_lcs_expanded_concepts') }} as ec
    group by ec.valueset_id
),

source_codes as (
    select
        oc.valueset_id,
        count(distinct oc.original_code) as code_count
    from {{ ref('stg_reference_ltc_lcs_original_codes') }} as oc
    group by oc.valueset_id
),

valueset_codes as (
    select
        vs.valueset_id,
        vs.valueset_friendly_name as valueset_name,
        coalesce(mc.code_count, 0) as mapped_code_count,
        coalesce(sc.code_count, 0) as source_code_count
    from {{ ref('stg_reference_ltc_lcs_valuesets') }} as vs
    left join mapped_codes mc on vs.valueset_id = mc.valueset_id
    left join source_codes sc on vs.valueset_id = sc.valueset_id
    where vs.valueset_id in (
        {% for vs in valuesets %}
            '{{ vs }}'{% if not loop.last %}, {% endif %}
        {% endfor %}
    )
    or upper(vs.valueset_friendly_name) in (
        {% for vs in valuesets %}
            upper('{{ vs }}'){% if not loop.last %}, {% endif %}
        {% endfor %}
    )
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
        coalesce(vc.mapped_code_count, 0) as mapped_code_count,
        coalesce(vc.source_code_count, 0) as source_code_count,
        case
            when vc.valueset_id is null then 'Valueset not found'
            when coalesce(vc.mapped_code_count, 0) = 0
                 and coalesce(vc.source_code_count, 0) = 0
                then 'Valueset has no codes via mapped or source paths'
            else null
        end as error_reason
    from expected_valuesets e
    left join valueset_codes vc
        on e.expected_valueset = vc.valueset_id
        or upper(e.expected_valueset) = upper(vc.valueset_name)
)

-- Return rows where valueset is missing or empty on both paths (test fails if any rows returned)
select
    expected_valueset,
    valueset_name,
    mapped_code_count,
    source_code_count,
    error_reason
from validation
where error_reason is not null

{% endtest %}
