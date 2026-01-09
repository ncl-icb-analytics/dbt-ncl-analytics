-- Raw layer model for reference_analyst_managed.DIAGNOSTIC_MSK_FLAG
-- Source: "DATA_LAKE__NCL"."ANALYST_MANAGED"
-- Description: Analyst-managed reference datasets and business rules
-- This is a 1:1 passthrough from source with standardized column names
select
    "NICIP_CODE" as nicip_code,
    "NICIP_DESCRIPTION" as nicip_description,
    "MODALITY" as modality,
    "EXCLUDED" as excluded
from {{ source('reference_analyst_managed', 'DIAGNOSTIC_MSK_FLAG') }}
