{{
    config(
        description="Raw layer (Reference data including PDS and lookup tables). 1:1 passthrough with cleaned column names. \nSource: Dictionary.dbo.OrganisationMatrixPractice \ndbt: source(''dictionary_dbo'', ''OrganisationMatrixPractice'') \nColumns:\n  SK_OrganisationID_Practice -> sk_organisation_id_practice\n  SK_OrganisationID_Network -> sk_organisation_id_network\n  SK_OrganisationID_Commissioner -> sk_organisation_id_commissioner\n  SK_OrganisationID_STP -> sk_organisation_id_stp"
    )
}}
select
    "SK_OrganisationID_Practice" as sk_organisation_id_practice,
    "SK_OrganisationID_Network" as sk_organisation_id_network,
    "SK_OrganisationID_Commissioner" as sk_organisation_id_commissioner,
    "SK_OrganisationID_STP" as sk_organisation_id_stp
from {{ source('dictionary_dbo', 'OrganisationMatrixPractice') }}
