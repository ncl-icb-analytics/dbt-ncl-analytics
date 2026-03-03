{{
    config(
        description="Raw layer (Analyst-managed reference datasets and business rules). 1:1 passthrough with cleaned column names. \nSource: DATA_LAKE__NCL.ANALYST_MANAGED.LSOA21_ICB \ndbt: source(''reference_analyst_managed'', ''LSOA21_ICB'') \nColumns:\n  LSOA21CD -> lsoa21_cd\n  LSOA21NM -> lsoa21_nm\n  SICBL23CD -> sicbl23_cd\n  SICBL23CDH -> sicbl23_cdh\n  SICBL23NM -> sicbl23_nm\n  ICB23CD -> icb23_cd\n  ICB23CDH -> icb23_cdh\n  ICB23NM -> icb23_nm\n  LAD23CD -> lad23_cd\n  LAD23NM -> lad23_nm\n  OBJECTID -> objectid"
    )
}}
select
    "LSOA21CD" as lsoa21_cd,
    "LSOA21NM" as lsoa21_nm,
    "SICBL23CD" as sicbl23_cd,
    "SICBL23CDH" as sicbl23_cdh,
    "SICBL23NM" as sicbl23_nm,
    "ICB23CD" as icb23_cd,
    "ICB23CDH" as icb23_cdh,
    "ICB23NM" as icb23_nm,
    "LAD23CD" as lad23_cd,
    "LAD23NM" as lad23_nm,
    "OBJECTID" as objectid
from {{ source('reference_analyst_managed', 'LSOA21_ICB') }}
