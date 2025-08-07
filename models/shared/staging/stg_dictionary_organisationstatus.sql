-- Staging model for dictionary.OrganisationStatus
-- Source: "Dictionary"."dbo"
-- Description: Reference data including PDS and lookup tables

select
    "SK_OrganisationStatusID" as sk_organisationstatusid,
    "BK_OrganisationStatus" as bk_organisationstatus,
    "OrganisationStatus" as organisationstatus
from {{ source('dictionary', 'OrganisationStatus') }}
