-- Raw layer model for reference_data_management.PRACTICE_SHORT_NAME
-- Source: "DATA_LAKE__NCL"."DATA_MANAGEMENT"
-- Description: Data management reference datasets
-- This is a 1:1 passthrough from source with standardized column names
select
    "PRACTICE_CODE" as practice_code,
    "PRACTICE_SHORT_NAME" as practice_short_name
from {{ source('reference_data_management', 'PRACTICE_SHORT_NAME') }}
