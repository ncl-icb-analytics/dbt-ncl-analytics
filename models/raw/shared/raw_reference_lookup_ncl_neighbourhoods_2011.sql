{{
    config(
        description="Raw layer (Analyst-managed reference datasets and business rules in the MODELLING environment). 1:1 passthrough with cleaned column names. \nSource: MODELLING.LOOKUP_NCL.NEIGHBOURHOODS_2011 \ndbt: source(''reference_lookup_ncl'', ''NEIGHBOURHOODS_2011'') \nColumns:\n  LSOA11CD -> lsoa11_cd\n  LSOA11NM -> lsoa11_nm\n  LSOA21CD_1 -> lsoa21_cd_1\n  LSOA21NM_1 -> lsoa21_nm_1\n  WD24CD -> wd24_cd\n  WD24NM -> wd24_nm\n  LAD24CD -> lad24_cd\n  LAD24NM -> lad24_nm\n  Neighbourhood -> neighbourhood"
    )
}}
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
