{{
    config(
        materialized = 'table',
        tags=['mhsds']
        )
}}


select
sk
,uniq_submission_id
,nhs_number_pseudo as sk_patient_id
--adding Person Death Date
,pers_death_date
,person_id
,local_patient_id
,org_id_prov
,dmic_ccg_code
from {{ref('raw_mhsds_mhs001mpi')}}