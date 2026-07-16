select
    deficit,
    detail_key,
    weight
from {{ ref('raw_aic_base_efi2_weights') }}
