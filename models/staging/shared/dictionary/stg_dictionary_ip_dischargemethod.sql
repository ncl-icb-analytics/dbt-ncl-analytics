select
    sk_discharge_method_id,
    bk_discharge_method_code,
    discharge_method_name
from {{ ref('raw_dictionary_ip_dischargemethod') }}
