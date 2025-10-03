select
    -- Primary key
    id,

    -- Business columns
    nhs_number_hash,
    title,
    gender_concept_id,
    birth_year,
    birth_month,
    death_year,
    death_month,

    -- Metadata
    lds_start_date_time

from {{ ref('raw_olids_person') }}
qualify row_number() over (
    partition by id
    order by lds_start_date_time desc
) = 1
