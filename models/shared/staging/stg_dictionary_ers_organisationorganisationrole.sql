-- Staging model for dictionary_eRS.OrganisationOrganisationRole
-- Source: "Dictionary"."E-Referral"
-- Description: Primary care referrals lookups

select
    "OrganisationID" as organisationid,
    "SK_OrganisationID" as sk_organisationid,
    "Role" as role
from {{ source('dictionary_eRS', 'OrganisationOrganisationRole') }}
