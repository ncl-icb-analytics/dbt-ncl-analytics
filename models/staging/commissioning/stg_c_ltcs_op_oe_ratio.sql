select
    patient_id,
    oe_ratio,
    predicted,
    op_att_tot_12_mo
from {{ ref('raw_c_ltcs_op_oe_ratio') }}
