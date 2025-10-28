-- Raw layer model for aic.BASE_SUS__ECDS_ATTENDANCE_REFERRED_TO
-- Source: "DATA_LAKE__NCL"."AIC_DEV"
-- Description: AIC pipelines
-- This is a 1:1 passthrough from source with standardized column names
select
    "ROWNUMBER_ID" as rownumber_id,
    "PRIMARYKEY_ID" as primarykey_id,
    "REFERRED_TO_ID" as referred_to_id,
    "SERVICE" as service,
    "IS_CODE_APPROVED" as is_code_approved,
    "REQUEST_DATE" as request_date,
    "REQUEST_TIME" as request_time,
    "ASSESSMENT_DATE" as assessment_date,
    "ASSESSMENT_TIME" as assessment_time,
    "DMIC_IMPORT_LOG_ID" as dmic_import_log_id,
    "REQUEST_TIMESTAMP" as request_timestamp,
    "ASSESSMENT_TIMESTAMP" as assessment_timestamp
from {{ source('aic', 'BASE_SUS__ECDS_ATTENDANCE_REFERRED_TO') }}
