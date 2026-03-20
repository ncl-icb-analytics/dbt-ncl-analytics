{{
    config(
        description="Raw layer (C-LTCS tables). 1:1 passthrough with cleaned column names. \nSource: DEV__PUBLISHED_REPORTING__DIRECT_CARE.C_LTCS.STATUS_LOG \ndbt: source(''c_ltcs'', ''STATUS_LOG'') \nColumns:\n  PATIENT_ID -> patient_id\n  PCN_CODE -> pcn_code\n  MDT_DATE -> mdt_date\n  ACTION -> action\n  ACTION_DATE -> action_date\n  CRITERIA -> criteria"
    )
}}
select
    "PATIENT_ID" as patient_id,
    "PCN_CODE" as pcn_code,
    "MDT_DATE" as mdt_date,
    "ACTION" as action,
    "ACTION_DATE" as action_date,
    "CRITERIA" as criteria
from {{ source('c_ltcs', 'STATUS_LOG') }}
