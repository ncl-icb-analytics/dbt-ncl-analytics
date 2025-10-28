{{
    config(materialized = 'view')
}}

select 
    uniq_care_cont_id
    , uniq_submission_id
    , person_id
    , care_cont_date
    , clin_cont_dur_of_care_cont
from {{ ref('raw_mhsds_mhs201carecontact') }}
qualify row_number() over (
    partition by uniq_care_cont_id
    order by effective_from desc
) = 1

-- PARTITION BY UniqServReqID, UniqCareContID