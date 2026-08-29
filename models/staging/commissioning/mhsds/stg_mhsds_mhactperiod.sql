{{
    config(
        materialized = 'table',
        tags=['mhsds']
    )
}}

with deduplicated as (
    {{
        select_latest_mhsds_state(
            mhsds_table = ref('raw_mhsds_mhs401mhactperiod'),
            partition_cols = ['uniq_mh_act_episode_id'],
            tie_breaker_cols = ['mhs401_uniq_id']
        )
    }}
)

-- REVIEW: raw MHS401 has no uniq_serv_req_id, so legal-status periods cannot
-- be linked directly to a referral.
select
    uniq_mh_act_episode_id
    , person_id
    , nhsd_legal_status
    , start_date_mh_act_legal_status_class
    , end_date_mh_act_legal_status_class
    , expiry_date_mh_act_legal_status_class
    , org_id_prov
    , unique_local_patient_id
from deduplicated
