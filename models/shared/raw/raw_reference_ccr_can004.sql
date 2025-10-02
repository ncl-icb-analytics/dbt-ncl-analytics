-- Raw layer model for reference_cancer_emis.CCR_CAN004
-- Source: "DATA_LAKE__NCL"."CANCER__EMIS"
-- Description: Cancer EMIS data
-- This is a 1:1 passthrough from source with standardized column names
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
from {{ source('reference_cancer_emis', 'CCR_CAN004') }}
