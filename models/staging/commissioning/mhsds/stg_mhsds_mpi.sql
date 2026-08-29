{{
    config(
        materialized = 'table',
        tags = ['mhsds']
    )
}}

with accepted_records as (
    {{ select_accepted_mhsds_period_records(ref('raw_mhsds_mhs001mpi')) }}
)

, deduplicated as (
    select *
    from accepted_records
    qualify row_number() over (
        partition by uniq_submission_id, unique_local_patient_id
        order by row_number desc, mhs001_uniq_id desc
    ) = 1
)

select
    sk
    , mhs001_uniq_id
    , unique_local_patient_id
    , uniq_submission_id
    , reporting_period_end_date::date as reporting_period_end_date
    , nhs_number_pseudo as sk_patient_id
    , pers_death_date
    , person_id
    , local_patient_id
    , org_id_prov
    , dmic_ccg_code
from deduplicated
