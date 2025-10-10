-- Raw layer model for reference_lookup_ncl.NEIGHBOURHOODS_2011
-- Source: "MODELLING"."LOOKUP_NCL"
-- Description: Analyst-managed reference datasets and business rules in the MODELLING environment
-- This is a 1:1 passthrough from source with standardized column names
select
    "LSOA11CD" as lsoa11_cd,
    "LSOA11NM" as lsoa11_nm,
    "LSOA21CD_1" as lsoa21_cd_1,
    "LSOA21NM_1" as lsoa21_nm_1,
    "WD24CD" as wd24_cd,
    "WD24NM" as wd24_nm,
    "LAD24CD" as lad24_cd,
    "LAD24NM" as lad24_nm,
    "Neighbourhood" as neighbourhood
from {{ source('reference_lookup_ncl', 'NEIGHBOURHOODS_2011') }}
