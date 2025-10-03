select
    -- Primary key
    id,

    -- Business columns
    array_agg(distinct nhs_number_hash) as nhs_number_hashes,
    max(title) as title,
    max(gender_concept_id) as gender_concept_id,
    max(birth_year) as birth_year,
    max(birth_month) as birth_month,
    max(death_year) as death_year,
    max(death_month) as death_month

from {{ ref('raw_olids_person') }}
group by id
