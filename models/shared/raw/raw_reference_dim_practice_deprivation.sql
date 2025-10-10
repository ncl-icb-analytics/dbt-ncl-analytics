-- Raw layer model for reference_lookup_ncl.DIM_PRACTICE_DEPRIVATION
-- Source: "MODELLING"."LOOKUP_NCL"
-- Description: Analyst-managed reference datasets and business rules in the MODELLING environment
-- This is a 1:1 passthrough from source with standardized column names
select
    "Practice_Code" as practice_code,
    "Practice_Name" as practice_name,
    "PCN" as pcn,
    "Borough" as borough,
    "Deprivation_Decile_2019_Fingertips" as deprivation_decile_2019_fingertips,
    "Deprivation_Quintile_2019_Fingertips" as deprivation_quintile_2019_fingertips
from {{ source('reference_lookup_ncl', 'DIM_PRACTICE_DEPRIVATION') }}
