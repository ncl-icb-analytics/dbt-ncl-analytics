-- Retain every referral/team key, including the unspecified team, and no extra keys.
with source_keys as (
    select unique_service_request_identifier, care_professional_team_local_identifier,
        1 as is_present
    from {{ ref('stg_csds_service_type_history') }}
    group by unique_service_request_identifier, care_professional_team_local_identifier
), fact_keys as (
    select unique_service_request_identifier, care_professional_team_local_identifier,
        1 as is_present
    from {{ ref('fct_csds_referral_service') }}
)
select
    coalesce(s.unique_service_request_identifier, f.unique_service_request_identifier) as unique_service_request_identifier
    , coalesce(s.care_professional_team_local_identifier, f.care_professional_team_local_identifier) as care_professional_team_local_identifier
    , iff(s.is_present is null, 'unexpected_in_fact', 'missing_from_fact') as discrepancy
from source_keys as s
full outer join fact_keys as f
    on s.unique_service_request_identifier = f.unique_service_request_identifier
    and s.care_professional_team_local_identifier is not distinct from f.care_professional_team_local_identifier
where s.is_present is null or f.is_present is null
