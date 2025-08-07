-- Staging model for dictionary.OrganisationMatrixCommissioner
-- Source: "Dictionary"."dbo"
-- Description: Reference data including PDS and lookup tables

select
    "SK_OrganisationID_Commissioner" as sk_organisationid_commissioner,
    "SK_OrganisationID_STP" as sk_organisationid_stp
from {{ source('dictionary', 'OrganisationMatrixCommissioner') }}
