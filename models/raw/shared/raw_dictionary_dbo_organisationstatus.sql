-- Raw layer model for dictionary_dbo.OrganisationStatus
-- Source: "Dictionary"."dbo"
-- Description: Reference data including PDS and lookup tables
-- This is a 1:1 passthrough from source with standardized column names
select
    "SK_OrganisationStatusID" as sk_organisation_status_id,
    "BK_OrganisationStatus" as bk_organisation_status,
    "OrganisationStatus" as organisation_status
from {{ source('dictionary_dbo', 'OrganisationStatus') }}
