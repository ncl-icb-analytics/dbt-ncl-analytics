-- One row for every submitted referral/team key, including the unspecified team.
with source_keys as (
    select unique_service_request_identifier, care_professional_team_local_identifier
    from {{ ref('stg_csds_service_type_history') }} group by 1, 2
)
select s.unique_service_request_identifier, s.care_professional_team_local_identifier
from source_keys as s
left join {{ ref('fct_csds_referral_service') }} as f
    on s.unique_service_request_identifier = f.unique_service_request_identifier
    and s.care_professional_team_local_identifier is not distinct from f.care_professional_team_local_identifier
where f.source_record_id is null
