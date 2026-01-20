{{
    config(
        description="Raw layer (Reference terminology data including SNOMED, BNF, and other code sets). 1:1 passthrough with cleaned column names. \nSource: DATA_LAKE__NCL.TERMINOLOGY.ECL_CLUSTER_CHANGES \ndbt: source(''reference_terminology'', ''ECL_CLUSTER_CHANGES'') \nColumns:\n  CHANGE_ID -> change_id\n  CLUSTER_ID -> cluster_id\n  CHANGE_TYPE -> change_type\n  CODE -> code\n  DISPLAY -> display\n  SYSTEM -> system\n  CHANGE_TIMESTAMP -> change_timestamp\n  CHANGED_BY -> changed_by\n  REFRESH_SESSION_ID -> refresh_session_id"
    )
}}
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
