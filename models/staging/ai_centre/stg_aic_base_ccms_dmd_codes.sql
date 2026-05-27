select
    conditionid,
    conditionname,
    productid,
    primaryterm
from {{ ref('raw_aic_base_ccms_dmd_codes') }}
