-- Raw layer model for reference_analyst_managed.FA__MONTHLY_REF_TRUST_VW
-- Source: "DATA_LAKE__NCL"."ANALYST_MANAGED"
-- Description: Analyst-managed reference datasets and business rules
-- This is a 1:1 passthrough from source with standardized column names
select
    "SK_ORGANISATIONID" as sk_organisationid,
    "TRUST_CODE" as trust_code,
    "TRUST_NAME" as trust_name,
    "TRUST_SHORTHAND" as trust_shorthand,
    "ORG_CODE" as org_code,
    "ORG_NAME" as org_name,
    "ORG_SHORTHAND" as org_shorthand,
    "GEO_TYPE" as geo_type
from {{ source('reference_analyst_managed', 'FA__MONTHLY_REF_TRUST_VW') }}
