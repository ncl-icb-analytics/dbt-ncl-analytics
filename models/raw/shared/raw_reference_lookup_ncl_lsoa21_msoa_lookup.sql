-- Raw layer model for reference_lookup_ncl.LSOA21_MSOA_LOOKUP
-- Source: "MODELLING"."LOOKUP_NCL"
-- Description: Analyst-managed reference datasets and business rules in the MODELLING environment
-- This is a 1:1 passthrough from source with standardized column names
select
    "LSOA21CD" as lsoa21_cd,
    "LSOA21NM" as lsoa21_nm,
    "MSOA21CD" as msoa21_cd,
    "MSOA21NM" as msoa21_nm,
    "LAD22CD" as lad22_cd,
    "LAD22NM" as lad22_nm
from {{ source('reference_lookup_ncl', 'LSOA21_MSOA_LOOKUP') }}
