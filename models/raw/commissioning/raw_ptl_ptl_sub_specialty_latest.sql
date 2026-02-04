{{
    config(
        description="Raw layer (ptl data). 1:1 passthrough with cleaned column names. \nSource: DATA_LAKE__NCL.PTL.PTL_SUB_SPECIALTY_LATEST \ndbt: source(''ptl'', ''PTL_SUB_SPECIALTY_LATEST'') \nColumns:\n  PROVIDER_CODE -> provider_code\n  LOCAL_SUB_SPECIALITY_CODE -> local_sub_speciality_code\n  LOCAL_SUB_SPECIALTY_DESCRIPTION -> local_sub_specialty_description\n  MAPPING_ID -> mapping_id"
    )
}}
select
    "PROVIDER_CODE" as provider_code,
    "LOCAL_SUB_SPECIALITY_CODE" as local_sub_speciality_code,
    "LOCAL_SUB_SPECIALTY_DESCRIPTION" as local_sub_specialty_description,
    "MAPPING_ID" as mapping_id
from {{ source('ptl', 'PTL_SUB_SPECIALTY_LATEST') }}
