{{
    config(
        description="Raw layer (Analyst-managed reference datasets and business rules). 1:1 passthrough with cleaned column names. \nSource: DATA_LAKE__NCL.ANALYST_MANAGED.FA__MONTHLY_REF_TRUST_VW \ndbt: source(''reference_analyst_managed'', ''FA__MONTHLY_REF_TRUST_VW'') \nColumns:\n  SK_ORGANISATIONID -> sk_organisationid\n  TRUST_CODE -> trust_code\n  TRUST_NAME -> trust_name\n  TRUST_SHORTHAND -> trust_shorthand\n  ORG_CODE -> org_code\n  ORG_NAME -> org_name\n  ORG_SHORTHAND -> org_shorthand\n  GEO_TYPE -> geo_type"
    )
}}
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
