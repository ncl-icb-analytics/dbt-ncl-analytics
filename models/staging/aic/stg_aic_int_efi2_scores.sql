select
    person_id,
    end_date,
    efi_score,
    category,
    age_at_end,
    gender,
    date_of_death
from {{ ref('raw_aic_int_efi2_scores') }}
-- Temporary dedup: <200 people have two rows after upstream person_id collapse
-- (old fragments rotated to one new person_id). Keep the higher (more-frail)
-- score; tie-break on most recent end_date. Remove once weaned off this dataset.
qualify row_number() over (
    partition by person_id
    order by efi_score desc, end_date desc
) = 1