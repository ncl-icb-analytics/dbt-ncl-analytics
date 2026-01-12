{{
    config(
        description="Raw layer (Analyst-managed reference datasets and business rules). 1:1 passthrough with cleaned column names. \nSource: DATA_LAKE__NCL.ANALYST_MANAGED.LSOA21_WARD25_LAD25 \ndbt: source(''reference_analyst_managed'', ''LSOA21_WARD25_LAD25'') \nColumns:\n  LSOA21CD -> lsoa21_cd\n  LSOA21NM -> lsoa21_nm\n  WD25CD -> wd25_cd\n  WD25NM -> wd25_nm\n  LAD25CD -> lad25_cd\n  LAD25NM -> lad25_nm\n  RESIDENT_FLAG -> resident_flag"
    )
}}
select
    "LSOA21CD" as lsoa21_cd,
    "LSOA21NM" as lsoa21_nm,
    "WD25CD" as wd25_cd,
    "WD25NM" as wd25_nm,
    "LAD25CD" as lad25_cd,
    "LAD25NM" as lad25_nm,
    "RESIDENT_FLAG" as resident_flag
from {{ source('reference_analyst_managed', 'LSOA21_WARD25_LAD25') }}
