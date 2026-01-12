{{
    config(
        description="Raw layer (Analyst-managed reference datasets and business rules in the MODELLING environment). 1:1 passthrough with cleaned column names. \nSource: MODELLING.LOOKUP_NCL.CONSULTANTCONNECT_ORG_GPCODE_LOOKUP \ndbt: source(''reference_lookup_ncl'', ''CONSULTANTCONNECT_ORG_GPCODE_LOOKUP'') \nColumns:\n  CleanOrganisation -> clean_organisation\n  OrganisationCode -> organisation_code"
    )
}}
select
    "CleanOrganisation" as clean_organisation,
    "OrganisationCode" as organisation_code
from {{ source('reference_lookup_ncl', 'CONSULTANTCONNECT_ORG_GPCODE_LOOKUP') }}
