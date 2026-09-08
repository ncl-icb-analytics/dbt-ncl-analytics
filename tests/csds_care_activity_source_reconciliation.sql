-- Compare occurrence keys so missing and unexpected rows cannot cancel in a count.
with source_keys as (
    select unique_submission_id, unique_care_activity_identifier, 1 as is_present
    from {{ ref('stg_csds_care_activity_history') }}
), fact_keys as (
    select unique_submission_id, unique_care_activity_identifier, 1 as is_present
    from {{ ref('fct_csds_care_activity') }}
)
select
    coalesce(s.unique_submission_id, f.unique_submission_id) as unique_submission_id
    , coalesce(s.unique_care_activity_identifier, f.unique_care_activity_identifier) as unique_care_activity_identifier
    , iff(s.is_present is null, 'unexpected_in_fact', 'missing_from_fact') as discrepancy
from source_keys as s
full outer join fact_keys as f
    on s.unique_submission_id = f.unique_submission_id
    and s.unique_care_activity_identifier = f.unique_care_activity_identifier
where s.is_present is null or f.is_present is null
