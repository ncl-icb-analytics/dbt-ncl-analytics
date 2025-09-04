-- Staging model for dictionary_dbo.OrganisationMatrixPractice
-- Source: "Dictionary"."dbo"
-- Description: Reference data including PDS and lookup tables

select
    "SK_OrganisationID_Practice" as sk_organisation_id_practice,
    "SK_OrganisationID_Network" as sk_organisation_id_network,
    "SK_OrganisationID_Commissioner" as sk_organisation_id_commissioner,
    "SK_OrganisationID_STP" as sk_organisation_id_stp
from {{ source('dictionary_dbo', 'OrganisationMatrixPractice') }}
