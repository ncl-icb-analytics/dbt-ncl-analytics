-- Raw layer model for c_ltcs.OP_OE_RATIO
-- Source: "DEV__PUBLISHED_REPORTING__DIRECT_CARE"."C_LTCS"
-- Description: C-LTCS tables
-- This is a 1:1 passthrough from source with standardized column names
select
    "PATIENT_ID" as patient_id,
    "OE_RATIO" as oe_ratio,
    "PREDICTED" as predicted,
    "OP_ATT_TOT_12MO" as op_att_tot_12_mo
from {{ source('c_ltcs', 'OP_OE_RATIO') }}
