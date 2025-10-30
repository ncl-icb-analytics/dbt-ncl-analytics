-- Raw layer model for c_ltcs.MDT_LOOKUP
-- Source: "DEV__PUBLISHED_REPORTING__DIRECT_CARE"."C_LTCS"
-- Description: C-LTCS tables
-- This is a 1:1 passthrough from source with standardized column names
select
    "PCN_CODE" as pcn_code,
    "MDT_DATE" as mdt_date
from {{ source('c_ltcs', 'MDT_LOOKUP') }}
