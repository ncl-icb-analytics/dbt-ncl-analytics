{{
    config(
        description="Raw layer (Cancer EMIS data). 1:1 passthrough with cleaned column names. \nSource: DATA_LAKE__NCL.CANCER__EMIS.CCR_CAN005 \ndbt: source(''reference_cancer_emis'', ''CCR_CAN005'') \nColumns:\n  Organisation -> organisation\n  CDB -> cdb\n  Population Count -> population_count\n  Parent -> parent\n  % -> percent\n  Males -> males\n  Females -> females\n  Excluded -> excluded\n  Status -> status\n  _Year -> year\n  _Month -> month\n  _TIMESTAMP -> timestamp"
    )
}}
select
    "Organisation" as organisation,
    "CDB" as cdb,
    "Population Count" as population_count,
    "Parent" as parent,
    "%" as percent,
    "Males" as males,
    "Females" as females,
    "Excluded" as excluded,
    "Status" as status,
    "_Year" as year,
    "_Month" as month,
    "_TIMESTAMP" as timestamp
from {{ source('reference_cancer_emis', 'CCR_CAN005') }}
