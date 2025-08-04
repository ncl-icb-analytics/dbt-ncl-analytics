-- Staging model for dictionary.PODGroups
-- Source: "Dictionary"."dbo"
{% if source.get('description') %}
-- Description: Reference data including PDS and lookup tables
{% endif %}

select
    "SK_PodGroupID" as sk_podgroupid,
    "PodDisplay" as poddisplay,
    "PodDataset" as poddataset,
    "PodMainGroup" as podmaingroup,
    "PodSubGroup" as podsubgroup,
    "DateCreated" as datecreated,
    "DateUpdated" as dateupdated
from {{ source('dictionary', 'PODGroups') }}
