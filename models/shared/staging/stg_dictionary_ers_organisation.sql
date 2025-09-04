-- Staging model for dictionary_eRS.Organisation
-- Source: "Dictionary"."E-Referral"
-- Description: Primary care referrals lookups

select
    "OrganisationID" as organisation_id,
    "ParentOrganisationID" as parent_organisation_id,
    "OrganisationRoleCode" as organisation_role_code,
    "OrganisationName" as organisation_name,
    "Postcode" as postcode,
    "SK_OrganisationID" as sk_organisation_id,
    "SK_OrganisationID_Parent" as sk_organisation_id_parent,
    "SK_PostcodeID" as sk_postcode_id
from {{ source('dictionary_eRS', 'Organisation') }}
