{{
    config(
        materialized = 'table',
        tags=['mhsds']
    )
}}

with mhs102 as (
    {{
        deduplicate_mhsds(
            mhsds_table = ref('raw_mhsds_mhs102servicetypereferredto'),
            partition_cols = ['uniq_serv_req_id']
        )
    }}
)

-- ~23% of referrals have no MHS102 row and carry the team type only on the
-- v6 MHS101 ServTeamType field
, mhs101 as (
    {{
        deduplicate_mhsds(
            mhsds_table = ref('raw_mhsds_mhs101referral'),
            partition_cols = ['uniq_serv_req_id']
        )
    }}
)

select
    r.uniq_serv_req_id
    , r.person_id
    , coalesce(s.serv_team_type_ref_to_mh, r.serv_team_type) as serv_team_type_ref_to_mh
    , coalesce(s.serv_team_int_age_group, r.serv_team_int_age_group) as serv_team_int_age_group
    , coalesce(s.service_type_name, r.service_type_name) as service_type_name
    , r.org_id_prov
    , r.uniq_submission_id
from mhs101 as r
left join mhs102 as s
    on r.uniq_serv_req_id = s.uniq_serv_req_id
