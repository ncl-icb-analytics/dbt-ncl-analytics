-- Public specification examples and aggregate-only failures protect interpretation.
with expected as (
    select column1 as concept_code, column2 as response_code,
        column3 as response_description, column4 as is_non_score_response
    from values
        ('987191000000101', '888', 'Not known', true),
        ('987191000000101', '999', 'Missing data', true),
        ('985981000000108', 'A', 'Phobic', false),
        ('979831000000108', 'B', 'Anxiety', false),
        ('763264000', '9', 'Terminally ill', false),
        ('747881000000109', 'NA', 'Not Applicable', true)
)
select 'published_response_meaning' as failure_reason, count(*) as failures
from expected as e
left join {{ ref('mhsds_assessment_response') }} as r
    on e.concept_code = r.concept_code and e.response_code = r.response_code
where e.response_description is distinct from r.response_description
    or e.is_non_score_response is distinct from r.is_non_score_response
having count(*) > 0

union all

select 'non_score_published_as_score', count(*)
from {{ ref('fct_mhsds_clinical_record') }}
where is_assessment_response_non_score and assessment_score_numeric is not null
having count(*) > 0

union all

select 'ambiguous_numeric_response_key', count(*)
from (
    select concept_code, numeric_response_value
    from {{ ref('mhsds_assessment_response') }}
    where numeric_response_value is not null
    group by concept_code, numeric_response_value
    having count(*) > 1
)
having count(*) > 0
