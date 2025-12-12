-- Raw layer model for reference_lookup_ncl.COMMISSIONED_SERVICE_CATEGORY
-- Source: "MODELLING"."LOOKUP_NCL"
-- Description: Analyst-managed reference datasets and business rules in the MODELLING environment
-- This is a 1:1 passthrough from source with standardized column names
select
    "COMMISSIONED_SERVICE_CATEGORY_CODE" as commissioned_service_category_code,
    "FULL_DESCRIPTION" as full_description,
    "SHORTER_DESCRIPTION" as shorter_description
from {{ source('reference_lookup_ncl', 'COMMISSIONED_SERVICE_CATEGORY') }}
