-- Raw layer model for reference_analyst_managed.CANCER__TRAJECTORY_SUBMISSIONS
-- Source: "DATA_LAKE__NCL"."ANALYST_MANAGED"
-- Description: Analyst-managed reference datasets and business rules
-- This is a 1:1 passthrough from source with standardized column names
select
    "COLLECTIONID" as collectionid,
    "SELECTEDICB" as selectedicb,
    "ASSOCIATEDORG" as associatedorg,
    "SECONDARYASSOCORG" as secondaryassocorg,
    "MEASUREID" as measureid,
    "MEASURETYPE" as measuretype,
    "MEASURESOURCE" as measuresource,
    "MEASURENAME" as measurename,
    "DIMENSIONID" as dimensionid,
    "DIMENSIONTYPE" as dimensiontype,
    "DIMENSIONNAME" as dimensionname,
    "DATA" as data,
    "COMMENTS" as comments
from {{ source('reference_analyst_managed', 'CANCER__TRAJECTORY_SUBMISSIONS') }}
