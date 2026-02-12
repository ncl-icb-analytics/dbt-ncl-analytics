{{
    config(materialized = 'table')
}}

WITH deduplicated AS (
{{
    deduplicate_mhsds(
        mhsds_table = ref('raw_mhsds_mhs201carecontact'),
        partition_cols = ['uniq_serv_req_id', 'uniq_care_cont_id']
    )
}} )

select 
    uniq_care_cont_id
    , uniq_serv_req_id
    , uniq_submission_id
    , person_id
    , care_cont_date
    , org_id_prov
    , attend_status
    , clin_cont_dur_of_care_cont
    , dm_icb_commissioner
from deduplicated