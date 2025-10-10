-- Raw layer model for dictionary_dbo.OrganisationMatrixCommissioner
-- Source: "Dictionary"."dbo"
-- Description: Reference data including PDS and lookup tables
-- This is a 1:1 passthrough from source with standardized column names
select
    "SK_OrganisationID_Commissioner" as sk_organisation_id_commissioner,
    "SK_OrganisationID_STP" as sk_organisation_id_stp
from {{ source('dictionary_dbo', 'OrganisationMatrixCommissioner') }}
