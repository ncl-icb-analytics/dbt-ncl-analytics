-- Raw layer model for aic.INT_GP_REGISTRATION
-- Source: "DATA_LAKE__NCL"."AIC_DEV"
-- Description: AIC pipelines
-- This is a 1:1 passthrough from source with standardized column names
select
    "GP_REGISTRATION_ID" as gp_registration_id,
    "PERSON_ID" as person_id,
    "ORGANISATION_CODE" as organisation_code,
    "ORGANISATION_NAME" as organisation_name,
    "PRIMARY_CARE_NETWORK" as primary_care_network,
    "REGISTRATION_START_DATE" as registration_start_date,
    "REGISTRATION_END_DATE" as registration_end_date,
    "REGISTRATION_DAYS" as registration_days,
    "IS_PRIMARY_ACTIVE_REGISTRATION" as is_primary_active_registration
from {{ source('aic', 'INT_GP_REGISTRATION') }}
