-- Raw layer model for aic.BASE_CCMS_DMD_CODES
-- Source: "DATA_LAKE__NCL"."AIC_DEV"
-- Description: AIC pipelines
-- This is a 1:1 passthrough from source with standardized column names
select
    "CONDITIONID" as conditionid,
    "CONDITIONNAME" as conditionname,
    "PRODUCTID" as productid,
    "PRIMARYTERM" as primaryterm
from {{ source('aic', 'BASE_CCMS_DMD_CODES') }}
