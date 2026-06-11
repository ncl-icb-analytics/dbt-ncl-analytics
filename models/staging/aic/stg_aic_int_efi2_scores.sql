select
    person_id,
    end_date,
    efi_score,
    category,
    age_at_end,
    gender,
    date_of_death
from {{ ref('raw_aic_int_efi2_scores') }}