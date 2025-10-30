-- Raw layer model for reference_lookup_ncl.LSOA_2021_WARD_2025_LOCAL_AUTHORITY_2025
-- Source: "MODELLING"."LOOKUP_NCL"
-- Description: Analyst-managed reference datasets and business rules in the MODELLING environment
-- This is a 1:1 passthrough from source with standardized column names
select
    "LSOA_2021_CODE" as lsoa_2021_code,
    "LSOA_2021_NAME" as lsoa_2021_name,
    "WARD_2025_CODE" as ward_2025_code,
    "WARD_2025_NAME" as ward_2025_name,
    "LOCAL_AUTHORITY_2025_CODE" as local_authority_2025_code,
    "LOCAL_AUTHORITY_2025_NAME" as local_authority_2025_name,
    "RESIDENT_FLAG" as resident_flag
from {{ source('reference_lookup_ncl', 'LSOA_2021_WARD_2025_LOCAL_AUTHORITY_2025') }}
