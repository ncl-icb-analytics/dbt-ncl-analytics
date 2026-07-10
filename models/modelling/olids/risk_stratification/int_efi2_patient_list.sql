{{
    config(
        materialized='table',
        cluster_by=['person_id'],
        tags=['efi2'])
}}

/*
eFI2 patient list — the living-patient spine and single as-at date for the eFI2
scoring run. One row per person.

Port of lds-pipelines stg_efi2__patient_list. Substitutions vs lds (OLIDS-native) unified patient tables
and date handling simplified to single date and fixed to current date.

Gender is normalised to FEMALE / MALE / OTHER-UNKNOWN exactly as lds does.
Deceased persons whose death is on/before the end date are excluded.
*/

with persons as (
    select
        bd.person_id,
        current_date() as end_date,
        bd.birth_date_approx as date_of_birth,
        bd.death_date_approx as date_of_death,
        dem.gender as gender_raw
    from {{ ref('dim_person_birth_death') }} bd
    left join {{ ref('dim_person_demographics') }} dem
        on bd.person_id = dem.person_id
)

select
    person_id,
    end_date,
    case
        when upper(gender_raw) in ('FEMALE', 'F')
        then 'FEMALE'
        when upper(gender_raw) in ('MALE', 'M')
        then 'MALE'
        else 'OTHER/UNKNOWN'
    end as gender,
    date_of_birth,
    date_of_death
from persons
where date_of_death is null or date_of_death > end_date
