{{
    config(
        description="Raw layer (Analyst-managed reference datasets and business rules). 1:1 passthrough with cleaned column names. \nSource: DATA_LAKE__NCL.ANALYST_MANAGED.FA__MONTHLY_SLAM_POD_GROUP_OVERVIEW \ndbt: source(''reference_analyst_managed'', ''FA__MONTHLY_SLAM_POD_GROUP_OVERVIEW'') \nColumns:\n  SLAMPOD -> slampod\n  PODGROUPOVERVIEW -> podgroupoverview"
    )
}}
select
    "SLAMPOD" as slampod,
    "PODGROUPOVERVIEW" as podgroupoverview
from {{ source('reference_analyst_managed', 'FA__MONTHLY_SLAM_POD_GROUP_OVERVIEW') }}
