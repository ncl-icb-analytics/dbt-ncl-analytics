-- Raw layer model for reference_lookup_ncl.OUTPUT_AREA_2021_LSOA_MSOA_LAD_DEC_2021
-- Source: "MODELLING"."LOOKUP_NCL"
-- Description: Analyst-managed reference datasets and business rules in the MODELLING environment
-- This is a 1:1 passthrough from source with standardized column names
select
    "OA21CD" as oa21_cd,
    "LSOA21CD" as lsoa21_cd,
    "LSOA21NM" as lsoa21_nm,
    "LSOA21NMW" as lsoa21_nmw,
    "MSOA21CD" as msoa21_cd,
    "MSOA21NM" as msoa21_nm,
    "MSOA21NMW" as msoa21_nmw,
    "LAD22CD" as lad22_cd,
    "LAD22NM" as lad22_nm,
    "LAD22NMW" as lad22_nmw,
    "OBJECTID" as objectid
from {{ source('reference_lookup_ncl', 'OUTPUT_AREA_2021_LSOA_MSOA_LAD_DEC_2021') }}
