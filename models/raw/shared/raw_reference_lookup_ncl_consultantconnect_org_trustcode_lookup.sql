{{
    config(
        description="Raw layer (Analyst-managed reference datasets and business rules in the MODELLING environment). 1:1 passthrough with cleaned column names. \nSource: MODELLING.LOOKUP_NCL.CONSULTANTCONNECT_ORG_TRUSTCODE_LOOKUP \ndbt: source(''reference_lookup_ncl'', ''CONSULTANTCONNECT_ORG_TRUSTCODE_LOOKUP'') \nColumns:\n  Trust -> trust\n  Organisation_Name -> organisation_name\n  Organisation_Code -> organisation_code"
    )
}}
select
    "Trust" as trust,
    "Organisation_Name" as organisation_name,
    "Organisation_Code" as organisation_code
from {{ source('reference_lookup_ncl', 'CONSULTANTCONNECT_ORG_TRUSTCODE_LOOKUP') }}
