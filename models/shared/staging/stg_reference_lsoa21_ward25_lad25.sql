-- Staging model for reference_analyst_managed.LSOA21_WARD25_LAD25
-- Source: "DATA_LAKE__NCL"."ANALYST_MANAGED"
-- Description: Analyst-managed reference datasets and business rules

select
    "LSOA21CD" as lsoa21_cd,
    "LSOA21NM" as lsoa21_nm,
    "WD25CD" as wd25_cd,
    "WD25NM" as wd25_nm,
    "LAD25CD" as lad25_cd,
    "LAD25NM" as lad25_nm,
    "RESIDENT_FLAG" as resident_flag
from {{ source('reference_analyst_managed', 'LSOA21_WARD25_LAD25') }}
