{{
    config(
        description="Raw layer (C-LTCS tables). 1:1 passthrough with cleaned column names. \nSource: DEV__PUBLISHED_REPORTING__DIRECT_CARE.C_LTCS.STATUS_LOG \ndbt: source(''c_ltcs'', ''STATUS_LOG'') \nColumns:\n  PATIENT_ID -> patient_id\n  AREA_CODE -> area_code\n  INTERVENTION_DATE -> intervention_date\n  ACTION -> action\n  ACTION_DATE -> action_date\n  DETAIL -> detail\n  INTERVENTION_NAME -> intervention_name"
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
