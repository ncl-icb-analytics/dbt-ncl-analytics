-- Staging model for dictionary_eRS.Organisation
-- Source: "Dictionary"."E-Referral"
-- Description: Primary care referrals lookups

select
    "OrganisationID" as organisationid,
    "ParentOrganisationID" as parentorganisationid,
    "OrganisationRoleCode" as organisationrolecode,
    "OrganisationName" as organisationname,
    "Postcode" as postcode,
    "SK_OrganisationID" as sk_organisationid,
    "SK_OrganisationID_Parent" as sk_organisationid_parent,
    "SK_PostcodeID" as sk_postcodeid
from {{ source('dictionary_eRS', 'Organisation') }}
