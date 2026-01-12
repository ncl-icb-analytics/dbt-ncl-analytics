{{
    config(
        description="Raw layer (Reference data including PDS and lookup tables). 1:1 passthrough with cleaned column names. \nSource: Dictionary.dbo.OrganisationMatrixCommissioner \ndbt: source(''dictionary_dbo'', ''OrganisationMatrixCommissioner'') \nColumns:\n  SK_OrganisationID_Commissioner -> sk_organisation_id_commissioner\n  SK_OrganisationID_STP -> sk_organisation_id_stp"
    )
}}
select
    "SK_OrganisationID_Commissioner" as sk_organisation_id_commissioner,
    "SK_OrganisationID_STP" as sk_organisation_id_stp
from {{ source('dictionary_dbo', 'OrganisationMatrixCommissioner') }}
