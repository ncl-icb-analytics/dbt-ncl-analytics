-- Raw layer model for aic.BASE_GOV__WARD_NAMES
-- Source: "DATA_LAKE__NCL"."AIC_DEV"
-- Description: AIC pipelines
-- This is a 1:1 passthrough from source with standardized column names
select
    "WARD_CODE" as ward_code,
    "WARD_NAME" as ward_name
from {{ source('aic', 'BASE_GOV__WARD_NAMES') }}
