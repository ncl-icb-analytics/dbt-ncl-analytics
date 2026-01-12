{{
    config(
        description="Raw layer (Primary care referrals lookups). 1:1 passthrough with cleaned column names. \nSource: Dictionary.E-Referral.OrganisationOrganisationRole \ndbt: source(''dictionary_eRS'', ''OrganisationOrganisationRole'') \nColumns:\n  OrganisationID -> organisation_id\n  SK_OrganisationID -> sk_organisation_id\n  Role -> role"
    )
}}
select
    "OrganisationID" as organisation_id,
    "SK_OrganisationID" as sk_organisation_id,
    "Role" as role
from {{ source('dictionary_eRS', 'OrganisationOrganisationRole') }}
