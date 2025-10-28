-- Raw layer model for aic.STG_GP__ORGANISATION
-- Source: "DATA_LAKE__NCL"."AIC_DEV"
-- Description: AIC pipelines
-- This is a 1:1 passthrough from source with standardized column names
select
    "ORGANISATION_ID" as organisation_id,
    "ORGANISATION_CODE" as organisation_code,
    "ORGANISATION_NAME" as organisation_name,
    "TYPE_CODE" as type_code,
    "TYPE_DESC" as type_desc,
    "PARENT_ORGANISATION_ID" as parent_organisation_id,
    "OPEN_DATE" as open_date,
    "CLOSE_DATE" as close_date,
    "IS_CLOSED" as is_closed
from {{ source('aic', 'STG_GP__ORGANISATION') }}
