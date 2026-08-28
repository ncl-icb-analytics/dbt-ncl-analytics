{{
    config(
        materialized = 'table',
        tags=['ers']
        )
    }}
select
    person_id,
    nhs_number_pseudo as sk_patient_id
from {{ ref('raw_ers_pc_bridging') }}
