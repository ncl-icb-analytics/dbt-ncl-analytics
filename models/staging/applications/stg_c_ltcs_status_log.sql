select patient_id
    , pcn_code
    , mdt_date
    , action
    , action_date
    , criteria
from {{ ref('raw_c_ltcs_status_log') }}