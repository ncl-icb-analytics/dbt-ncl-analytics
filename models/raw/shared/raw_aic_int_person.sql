-- Raw layer model for aic.INT_PERSON
-- Source: "DATA_LAKE__NCL"."AIC_DEV"
-- Description: AIC pipelines
-- This is a 1:1 passthrough from source with standardized column names
select
    "PERSON_ID" as person_id,
    "BIRTH_YEAR" as birth_year,
    "BIRTH_MONTH" as birth_month,
    "DEATH_YEAR" as death_year,
    "DEATH_MONTH" as death_month,
    "GENDER_CONCEPT_CODE" as gender_concept_code,
    "GENDER_CONCEPT_NAME" as gender_concept_name,
    "ETHNICITY_CONCEPT_CODE" as ethnicity_concept_code,
    "ETHNICITY_CONCEPT_NAME" as ethnicity_concept_name,
    "RESIDENCE_WARD_CODE" as residence_ward_code,
    "RESIDENCE_WARD_NAME" as residence_ward_name,
    "RESIDENCE_BOROUGH_CODE" as residence_borough_code,
    "RESIDENCE_BOROUGH_NAME" as residence_borough_name,
    "RESIDENCE_UPRN" as residence_uprn,
    "RESIDENCE_LSOA" as residence_lsoa,
    "RESIDENCE_MSOA" as residence_msoa,
    "IMD_DECILE" as imd_decile,
    "IMD_QUINTILE" as imd_quintile,
    "IS_TYPE_1_OPTOUT" as is_type_1_optout,
    "IS_DEAD" as is_dead,
    "IS_ACTIVE_REGISTERED" as is_active_registered
from {{ source('aic', 'INT_PERSON') }}
