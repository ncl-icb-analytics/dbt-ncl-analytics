{{
    config(
        description="Raw layer (Analyst-managed reference datasets and business rules in the MODELLING environment). 1:1 passthrough with cleaned column names. \nSource: MODELLING.LOOKUP_NCL.NCL_PROVIDER \ndbt: source(''reference_lookup_ncl'', ''NCL_PROVIDER'') \nColumns:\n  SK_ORGANISATION_ID -> sk_organisation_id\n  PROVIDER_CODE -> provider_code\n  PROVIDER_NAME -> provider_name\n  PROVIDER_SHORTHAND -> provider_shorthand\n  REPORTING_CODE -> reporting_code\n  REPORTING_NAME -> reporting_name\n  REPORTING_SHORTHAND -> reporting_shorthand\n  PROVIDER_TYPE -> provider_type\n  ROW_TYPE -> row_type"
    )
}}
select
    "SK_ORGANISATION_ID" as sk_organisation_id,
    "PROVIDER_CODE" as provider_code,
    "PROVIDER_NAME" as provider_name,
    "PROVIDER_SHORTHAND" as provider_shorthand,
    "REPORTING_CODE" as reporting_code,
    "REPORTING_NAME" as reporting_name,
    "REPORTING_SHORTHAND" as reporting_shorthand,
    "PROVIDER_TYPE" as provider_type,
    "ROW_TYPE" as row_type
from {{ source('reference_lookup_ncl', 'NCL_PROVIDER') }}
