select cast(patient_id as varchar) as patient_id
    , area_code
    , intervention_date
    , action
    , action_date
    , detail
    , intervention_name
from {{ ref('raw_c_ltcs_status_log') }}