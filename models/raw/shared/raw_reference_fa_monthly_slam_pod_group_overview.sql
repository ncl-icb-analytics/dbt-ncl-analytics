-- Raw layer model for reference_analyst_managed.FA__MONTHLY_SLAM_POD_GROUP_OVERVIEW
-- Source: "DATA_LAKE__NCL"."ANALYST_MANAGED"
-- Description: Analyst-managed reference datasets and business rules
-- This is a 1:1 passthrough from source with standardized column names
select
    "SLAMPOD" as slampod,
    "PODGROUPOVERVIEW" as podgroupoverview
from {{ source('reference_analyst_managed', 'FA__MONTHLY_SLAM_POD_GROUP_OVERVIEW') }}
