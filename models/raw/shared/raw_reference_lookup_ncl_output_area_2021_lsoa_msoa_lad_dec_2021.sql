{{
    config(
        description="Raw layer (Analyst-managed reference datasets and business rules in the MODELLING environment). 1:1 passthrough with cleaned column names. \nSource: MODELLING.LOOKUP_NCL.OUTPUT_AREA_2021_LSOA_MSOA_LAD_DEC_2021 \ndbt: source(''reference_lookup_ncl'', ''OUTPUT_AREA_2021_LSOA_MSOA_LAD_DEC_2021'') \nColumns:\n  OA21CD -> oa21_cd\n  LSOA21CD -> lsoa21_cd\n  LSOA21NM -> lsoa21_nm\n  LSOA21NMW -> lsoa21_nmw\n  MSOA21CD -> msoa21_cd\n  MSOA21NM -> msoa21_nm\n  MSOA21NMW -> msoa21_nmw\n  LAD22CD -> lad22_cd\n  LAD22NM -> lad22_nm\n  LAD22NMW -> lad22_nmw\n  OBJECTID -> objectid"
    )
}}
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
