-- Staging model for dictionary_dbo.OrganisationFormerName
-- Source: "Dictionary"."dbo"
-- Description: Reference data including PDS and lookup tables

select
    "SK_OrganisationID" as sk_organisationid,
    "Organisation_Name" as organisation_name,
    "StartDate" as startdate,
    "EndDate" as enddate
from {{ source('dictionary_dbo', 'OrganisationFormerName') }}
