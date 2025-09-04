-- Staging model for dictionary_dbo.OrganisationStatus
-- Source: "Dictionary"."dbo"
-- Description: Reference data including PDS and lookup tables

select
    "SK_OrganisationStatusID" as sk_organisation_status_id,
    "BK_OrganisationStatus" as bk_organisation_status,
    "OrganisationStatus" as organisation_status
from {{ source('dictionary_dbo', 'OrganisationStatus') }}
