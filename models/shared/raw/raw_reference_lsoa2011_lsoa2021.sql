-- Raw layer model for reference_analyst_managed.LSOA2011_LSOA2021
-- Source: "DATA_LAKE__NCL"."ANALYST_MANAGED"
-- Description: Analyst-managed reference datasets and business rules
-- This is a 1:1 passthrough from source with standardized column names
select
    "LSOA11CD" as lsoa11_cd,
    "LSOA11NM" as lsoa11_nm,
    "LSOA21CD" as lsoa21_cd,
    "LSOA21NM" as lsoa21_nm,
    "CHGIND" as chgind,
    "LAD22CD" as lad22_cd,
    "LAD22NM" as lad22_nm,
    "LAD22NMW" as lad22_nmw,
    "OBJECTID" as objectid
from {{ source('reference_analyst_managed', 'LSOA2011_LSOA2021') }}
