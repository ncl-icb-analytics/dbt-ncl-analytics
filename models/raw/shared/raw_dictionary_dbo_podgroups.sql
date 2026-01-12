{{
    config(
        description="Raw layer (Reference data including PDS and lookup tables). 1:1 passthrough with cleaned column names. \nSource: Dictionary.dbo.PODGroups \ndbt: source(''dictionary_dbo'', ''PODGroups'') \nColumns:\n  SK_PodGroupID -> sk_pod_group_id\n  PodDisplay -> pod_display\n  PodDataset -> pod_dataset\n  PodMainGroup -> pod_main_group\n  PodSubGroup -> pod_sub_group\n  DateCreated -> date_created\n  DateUpdated -> date_updated"
    )
}}
select
    "SK_PodGroupID" as sk_pod_group_id,
    "PodDisplay" as pod_display,
    "PodDataset" as pod_dataset,
    "PodMainGroup" as pod_main_group,
    "PodSubGroup" as pod_sub_group,
    "DateCreated" as date_created,
    "DateUpdated" as date_updated
from {{ source('dictionary_dbo', 'PODGroups') }}
