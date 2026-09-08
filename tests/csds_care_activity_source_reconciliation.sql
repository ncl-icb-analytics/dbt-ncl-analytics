-- Every accepted activity occurrence remains once after parent and terminology joins.
with source_counts as (
    select unique_submission_id, count(*) as n
    from {{ ref('stg_csds_care_activity_history') }} group by 1
), fact_counts as (
    select unique_submission_id, count(*) as n
    from {{ ref('fct_csds_care_activity') }} group by 1
)
select coalesce(s.unique_submission_id, f.unique_submission_id) as unique_submission_id
from source_counts as s full outer join fact_counts as f
    on s.unique_submission_id = f.unique_submission_id
where coalesce(s.n, 0) <> coalesce(f.n, 0)
