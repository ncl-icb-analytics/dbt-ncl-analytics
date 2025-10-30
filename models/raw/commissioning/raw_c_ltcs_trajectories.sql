-- Raw layer model for c_ltcs.TRAJECTORIES
-- Source: "DEV__PUBLISHED_REPORTING__DIRECT_CARE"."C_LTCS"
-- Description: C-LTCS tables
-- This is a 1:1 passthrough from source with standardized column names
select
    "PATIENT_ID" as patient_id,
    "AE_ENCOUNTERS_SL" as ae_encounters_sl,
    "IP_ENCOUNTERS_SL" as ip_encounters_sl,
    "OP_ENCOUNTERS_SL" as op_encounters_sl,
    "GP_ENCOUNTERS_SL" as gp_encounters_sl
from {{ source('c_ltcs', 'TRAJECTORIES') }}
