-- Raw layer model for aic.INT_CCMS_CURRENT
-- Source: "DATA_LAKE__NCL"."AIC_DEV"
-- Description: AIC pipelines
-- This is a 1:1 passthrough from source with standardized column names
select
    "CCMS_CURRENT_ID" as ccms_current_id,
    "PERSON_ID" as person_id,
    "CAMBRIDGE_COMORBIDITY_SCORE" as cambridge_comorbidity_score,
    "LAST_UPDATED" as last_updated
from {{ source('aic', 'INT_CCMS_CURRENT') }}
