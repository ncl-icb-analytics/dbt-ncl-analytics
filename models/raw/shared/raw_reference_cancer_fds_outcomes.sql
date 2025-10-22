-- Raw layer model for reference_analyst_managed.CANCER__FDS_OUTCOMES
-- Source: "DATA_LAKE__NCL"."ANALYST_MANAGED"
-- Description: Analyst-managed reference datasets and business rules
-- This is a 1:1 passthrough from source with standardized column names
select
    "PERIOD" as period,
    "YEAR" as year,
    "MONTH" as month,
    "STANDARD" as standard,
    "ORG_CODE" as org_code,
    "STAGE_ROUTE" as stage_route,
    "FDS_END_REASON" as fds_end_reason,
    "CANCER_TYPE" as cancer_type,
    "TOTAL_TREATED" as total_treated,
    "WITHIN_STANDARD" as within_standard,
    "BREACHES" as breaches
from {{ source('reference_analyst_managed', 'CANCER__FDS_OUTCOMES') }}
