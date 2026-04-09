{{
    config(
        description="Raw layer (C-LTCS tables). 1:1 passthrough with cleaned column names. \nSource: DEV__PUBLISHED_REPORTING__DIRECT_CARE.C_LTCS.STATUS_LOG"
    )
}}
select
    "PATIENT_ID" as patient_id,
    "AREA_CODE" as area_code,
    "INTERVENTION_DATE" as intervention_date,
    "ACTION" as action,
    "ACTION_DATE" as action_date,
    "DETAIL" as detail,
    "INTERVENTION_NAME" as intervention_name
from {{ source('c_ltcs', 'STATUS_LOG') }}
