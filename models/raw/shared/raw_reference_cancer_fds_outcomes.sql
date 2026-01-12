{{
    config(
        description="Raw layer (Analyst-managed reference datasets and business rules). 1:1 passthrough with cleaned column names. \nSource: DATA_LAKE__NCL.ANALYST_MANAGED.CANCER__FDS_OUTCOMES \ndbt: source(''reference_analyst_managed'', ''CANCER__FDS_OUTCOMES'') \nColumns:\n  PERIOD -> period\n  YEAR -> year\n  MONTH -> month\n  STANDARD -> standard\n  ORG_CODE -> org_code\n  STAGE_ROUTE -> stage_route\n  FDS_END_REASON -> fds_end_reason\n  CANCER_TYPE -> cancer_type\n  TOTAL_TREATED -> total_treated\n  WITHIN_STANDARD -> within_standard\n  BREACHES -> breaches"
    )
}}
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
