-- Staging model for dictionary_dbo.OrganisationHierarchyPractice
-- Source: "Dictionary"."dbo"
-- Description: Reference data including PDS and lookup tables

select
    "SK_OrganisationID" as sk_organisationid,
    "SK_OrganisationID_Parent" as sk_organisationid_parent,
    "Level" as level
from {{ source('dictionary_dbo', 'OrganisationHierarchyPractice') }}
