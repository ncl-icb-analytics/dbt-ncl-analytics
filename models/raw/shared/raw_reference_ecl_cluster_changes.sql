-- Raw layer model for reference_terminology.ECL_CLUSTER_CHANGES
-- Source: "DATA_LAKE__NCL"."TERMINOLOGY"
-- Description: Reference terminology data including SNOMED, BNF, and other code sets
-- This is a 1:1 passthrough from source with standardized column names
select
    "CHANGE_ID" as change_id,
    "CLUSTER_ID" as cluster_id,
    "CHANGE_TYPE" as change_type,
    "CODE" as code,
    "DISPLAY" as display,
    "SYSTEM" as system,
    "CHANGE_TIMESTAMP" as change_timestamp,
    "CHANGED_BY" as changed_by,
    "REFRESH_SESSION_ID" as refresh_session_id
from {{ source('reference_terminology', 'ECL_CLUSTER_CHANGES') }}
