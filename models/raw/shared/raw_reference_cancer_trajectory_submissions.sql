{{
    config(
        description="Raw layer (Analyst-managed reference datasets and business rules). 1:1 passthrough with cleaned column names. \nSource: DATA_LAKE__NCL.ANALYST_MANAGED.CANCER__TRAJECTORY_SUBMISSIONS \ndbt: source(''reference_analyst_managed'', ''CANCER__TRAJECTORY_SUBMISSIONS'') \nColumns:\n  COLLECTIONID -> collectionid\n  SELECTEDICB -> selectedicb\n  ASSOCIATEDORG -> associatedorg\n  SECONDARYASSOCORG -> secondaryassocorg\n  MEASURETYPE -> measuretype\n  MEASURESOURCE -> measuresource\n  MEASURENAME -> measurename\n  DIMENSIONID -> dimensionid\n  DIMENSIONTYPE -> dimensiontype\n  DIMENSIONNAME -> dimensionname\n  DATA -> data\n  COMMENTS -> comments\n  MEASUREID -> measureid"
    )
}}
select
    "COLLECTIONID" as collectionid,
    "SELECTEDICB" as selectedicb,
    "ASSOCIATEDORG" as associatedorg,
    "SECONDARYASSOCORG" as secondaryassocorg,
    "MEASURETYPE" as measuretype,
    "MEASURESOURCE" as measuresource,
    "MEASURENAME" as measurename,
    "DIMENSIONID" as dimensionid,
    "DIMENSIONTYPE" as dimensiontype,
    "DIMENSIONNAME" as dimensionname,
    "DATA" as data,
    "COMMENTS" as comments,
    "MEASUREID" as measureid
from {{ source('reference_analyst_managed', 'CANCER__TRAJECTORY_SUBMISSIONS') }}
