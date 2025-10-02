-- Raw layer model for dictionary_eRS.OrganisationOrganisationRole
-- Source: "Dictionary"."E-Referral"
-- Description: Primary care referrals lookups
-- This is a 1:1 passthrough from source with standardized column names
select
    "OrganisationID" as organisation_id,
    "SK_OrganisationID" as sk_organisation_id,
    "Role" as role
from {{ source('dictionary_eRS', 'OrganisationOrganisationRole') }}
