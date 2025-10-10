-- Raw layer model for dictionary_dbo.PODGroups
-- Source: "Dictionary"."dbo"
-- Description: Reference data including PDS and lookup tables
-- This is a 1:1 passthrough from source with standardized column names
select
    "SK_PodGroupID" as sk_pod_group_id,
    "PodDisplay" as pod_display,
    "PodDataset" as pod_dataset,
    "PodMainGroup" as pod_main_group,
    "PodSubGroup" as pod_sub_group,
    "DateCreated" as date_created,
    "DateUpdated" as date_updated
from {{ source('dictionary_dbo', 'PODGroups') }}
