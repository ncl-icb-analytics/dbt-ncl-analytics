-- Raw layer model for c_ltcs.STATUS_LOG
-- Source: "DEV__PUBLISHED_REPORTING__DIRECT_CARE"."C_LTCS"
-- Description: C-LTCS tables
-- This is a 1:1 passthrough from source with standardized column names
select
    "PATIENT_ID" as patient_id,
    "PCN_CODE" as pcn_code,
    "MDT_DATE" as mdt_date,
    "ACTION" as action,
    "ACTION_DATE" as action_date,
    "CRITERIA" as criteria
from {{ source('c_ltcs', 'STATUS_LOG') }}
