{{
    config(
        description="Raw layer (Analyst-managed reference datasets and business rules in the MODELLING environment). 1:1 passthrough with cleaned column names. \nSource: MODELLING.LOOKUP_NCL.LSOA_2021_WARD_2025_LOCAL_AUTHORITY_2025 \ndbt: source(''reference_lookup_ncl'', ''LSOA_2021_WARD_2025_LOCAL_AUTHORITY_2025'') \nColumns:\n  LSOA_2021_CODE -> lsoa_2021_code\n  LSOA_2021_NAME -> lsoa_2021_name\n  WARD_2025_CODE -> ward_2025_code\n  WARD_2025_NAME -> ward_2025_name\n  LOCAL_AUTHORITY_2025_CODE -> local_authority_2025_code\n  LOCAL_AUTHORITY_2025_NAME -> local_authority_2025_name\n  RESIDENT_FLAG -> resident_flag"
    )
}}
select
    "LSOA_2021_CODE" as lsoa_2021_code,
    "LSOA_2021_NAME" as lsoa_2021_name,
    "WARD_2025_CODE" as ward_2025_code,
    "WARD_2025_NAME" as ward_2025_name,
    "LOCAL_AUTHORITY_2025_CODE" as local_authority_2025_code,
    "LOCAL_AUTHORITY_2025_NAME" as local_authority_2025_name,
    "RESIDENT_FLAG" as resident_flag
from {{ source('reference_lookup_ncl', 'LSOA_2021_WARD_2025_LOCAL_AUTHORITY_2025') }}
