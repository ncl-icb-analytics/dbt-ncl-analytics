select patient_id
    , pcn_code
    , mdt_date
    , action
    , action_date
    , criteria
from {{ ref('stg_c_ltcs_status_log') }}
qualify row_number() over (partition by patient_id order by action_date desc) = 1