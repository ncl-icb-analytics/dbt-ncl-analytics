with latest as (
    select *
    from {{ ref('mhsds_assessment_scale_definitions') }}
    qualify specification_version = max(specification_version) over (partition by concept_code)
)

select
    concept_code
    , min(assessment_tool_name) as assessment_tool_name
    , min(assessment_description) as assessment_description
    , listagg(distinct published_value, '; ') within group (order by published_value)
        as published_values
    , min(try_to_number(decimal_places)) as decimal_places
    , count_if(regexp_like(published_value, '-?[0-9]+([.][0-9]+)? *- *-?[0-9]+([.][0-9]+)?'))
        as numeric_range_count
    , min(iff(regexp_like(published_value, '-?[0-9]+([.][0-9]+)? *- *-?[0-9]+([.][0-9]+)?'),
        try_to_decimal(regexp_substr(published_value, '^-?[0-9]+([.][0-9]+)?'), 38, 9), null))
        as minimum_numeric_value
    , max(iff(regexp_like(published_value, '-?[0-9]+([.][0-9]+)? *- *-?[0-9]+([.][0-9]+)?'),
        try_to_decimal(regexp_substr(published_value, '[0-9]+([.][0-9]+)?$'), 38, 9), null))
        as maximum_numeric_value
    , min(collection_start_date) as collection_start_date
    , max(specification_version) as specification_version
from latest
group by concept_code
