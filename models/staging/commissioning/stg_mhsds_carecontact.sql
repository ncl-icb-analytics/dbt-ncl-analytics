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
    --adding additional fields to help categorise care contacts.
    , care_prof_team_local_id
    , site_id_of_treat
    , cons_type 
    , cons_mechanism_mh
    , act_loc_type_code
from deduplicated