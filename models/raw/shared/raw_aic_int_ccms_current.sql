{{
    config(
        description="Raw layer (AIC pipelines). 1:1 passthrough with cleaned column names. \nSource: DATA_LAKE__NCL.AIC_DEV.INT_CCMS_CURRENT \ndbt: source(''aic'', ''INT_CCMS_CURRENT'') \nColumns:\n  PERSON_ID -> person_id\n  CAMBRIDGE_COMORBIDITY_SCORE -> cambridge_comorbidity_score\n  CCMS_CURRENT_ID -> ccms_current_id\n  LAST_UPDATED -> last_updated"
    )
}}
select
    "PERSON_ID" as person_id,
    "CAMBRIDGE_COMORBIDITY_SCORE" as cambridge_comorbidity_score,
    "CCMS_CURRENT_ID" as ccms_current_id,
    "LAST_UPDATED" as last_updated
from {{ source('aic', 'INT_CCMS_CURRENT') }}
