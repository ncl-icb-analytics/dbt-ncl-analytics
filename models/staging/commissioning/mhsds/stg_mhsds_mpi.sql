{{
    config(
        materialized = 'view',
        tags = ['mhsds']
    )
}}

with active_records as (
    {{ select_active_mhsds_records(ref('raw_mhsds_mhs001mpi')) }}
)

select
    sk
    , uniq_submission_id
    , nhs_number_pseudo as sk_patient_id
    , pers_death_date
    , person_id
    , local_patient_id
    , org_id_prov
    , dmic_ccg_code
from active_records
