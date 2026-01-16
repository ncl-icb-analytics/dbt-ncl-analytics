select
    pcn_code,
    mdt_date
from {{ ref('raw_c_ltcs_mdt_lookup') }}
