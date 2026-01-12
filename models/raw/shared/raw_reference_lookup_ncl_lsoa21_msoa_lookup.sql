{{
    config(
        description="Raw layer (Analyst-managed reference datasets and business rules in the MODELLING environment). 1:1 passthrough with cleaned column names. \nSource: MODELLING.LOOKUP_NCL.LSOA21_MSOA_LOOKUP \ndbt: source(''reference_lookup_ncl'', ''LSOA21_MSOA_LOOKUP'') \nColumns:\n  LSOA21CD -> lsoa21_cd\n  LSOA21NM -> lsoa21_nm\n  MSOA21CD -> msoa21_cd\n  MSOA21NM -> msoa21_nm\n  LAD22CD -> lad22_cd\n  LAD22NM -> lad22_nm"
    )
}}
select
    "LSOA21CD" as lsoa21_cd,
    "LSOA21NM" as lsoa21_nm,
    "MSOA21CD" as msoa21_cd,
    "MSOA21NM" as msoa21_nm,
    "LAD22CD" as lad22_cd,
    "LAD22NM" as lad22_nm
from {{ source('reference_lookup_ncl', 'LSOA21_MSOA_LOOKUP') }}
