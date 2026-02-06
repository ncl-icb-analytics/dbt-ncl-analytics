{{
    config(
        description="Raw layer (ptl data). 1:1 passthrough with cleaned column names. \nSource: DATA_LAKE__NCL.PTL.PTL_SUBSPECIALTY_MAPPING \ndbt: source(''ptl'', ''PTL_SUBSPECIALTY_MAPPING'') \nColumns:\n  PROVIDER_CODE -> provider_code\n  LOCAL_SUBSPECIALITY_CODE -> local_subspeciality_code\n  LOCAL_SUBSPECIALTY_DESCRIPTION -> local_subspecialty_description"
    )
}}
select
    "PROVIDER_CODE" as provider_code,
    "LOCAL_SUBSPECIALITY_CODE" as local_subspeciality_code,
    "LOCAL_SUBSPECIALTY_DESCRIPTION" as local_subspecialty_description
from {{ source('ptl', 'PTL_SUBSPECIALTY_MAPPING') }}
