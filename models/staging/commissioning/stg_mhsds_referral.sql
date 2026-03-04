{{
    config(materialized = 'table')
}}

WITH deduplicated AS (
{{
    deduplicate_mhsds(
        mhsds_table = ref('raw_mhsds_mhs101referral'),
        partition_cols = ['uniq_serv_req_id']
    )
}} )

select 
    uniq_serv_req_id
    , person_id
    , mhs101_uniq_id
    , org_id_prov
    , org_id_referring
    , prim_reason_referral_mh
    , decision_to_treat_date
    , referral_request_received_date
    , referring_care_professional_type
    , refer_rejection_date
    , refer_reject_reason
    , refer_clos_reason
from deduplicated 