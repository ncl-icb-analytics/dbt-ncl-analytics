-- Raw layer model for aic.STG_NHSBSA__APID_VPID
-- Source: "DATA_LAKE__NCL"."AIC_DEV"
-- Description: AIC pipelines
-- This is a 1:1 passthrough from source with standardized column names
select
    "APID" as apid,
    "VPID" as vpid
from {{ source('aic', 'STG_NHSBSA__APID_VPID') }}
