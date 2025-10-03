-- Raw layer model for dictionary_dbo.OrganisationONSCode
-- Source: "Dictionary"."dbo"
-- Description: Reference data including PDS and lookup tables
-- This is a 1:1 passthrough from source with standardized column names
select
    "SK_OrganisationID" as sk_organisation_id,
    "ONS_Code" as ons_code
from {{ source('dictionary_dbo', 'OrganisationONSCode') }}
