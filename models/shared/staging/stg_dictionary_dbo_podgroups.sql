-- Staging model for dictionary_dbo.PODGroups
-- Source: "Dictionary"."dbo"
-- Description: Reference data including PDS and lookup tables

select
    "SK_PodGroupID" as sk_podgroupid,
    "PodDisplay" as poddisplay,
    "PodDataset" as poddataset,
    "PodMainGroup" as podmaingroup,
    "PodSubGroup" as podsubgroup,
    "DateCreated" as datecreated,
    "DateUpdated" as dateupdated
from {{ source('dictionary_dbo', 'PODGroups') }}
