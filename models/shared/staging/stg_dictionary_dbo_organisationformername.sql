-- Staging model for dictionary_dbo.OrganisationFormerName
-- Source: "Dictionary"."dbo"
-- Description: Reference data including PDS and lookup tables

select
    "SK_OrganisationID" as sk_organisation_id,
    "Organisation_Name" as organisation_name,
    "StartDate" as start_date,
    "EndDate" as end_date
from {{ source('dictionary_dbo', 'OrganisationFormerName') }}
