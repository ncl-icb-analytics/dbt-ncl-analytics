{{ config(materialized='table', tags=['mhsds']) }}

select
    t.mhs902_uniq_id
    , t.care_prof_team_local_id
    , t.uniq_care_prof_team_local_id
    , t.org_id_care_prof_team_local_id
    , t.org_id_prov
    , t.serv_team_type_mh
    , t.service_type_name
    , t.serv_team_int_age_group
    , t.uniq_submission_id
    , t.uniq_month_id
    , t.reporting_period_start_date
    , t.reporting_period_end_date
    , t.dmic_dataset
    , t.effective_from
    , t.dmic_date_added
from {{ ref('raw_mhsds_mhs902serviceteamdetails') }} as t
inner join {{ ref('stg_mhsds_activesubmission') }} as a
    on t.uniq_submission_id = a.uniq_submission_id
