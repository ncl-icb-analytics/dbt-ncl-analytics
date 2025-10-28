-- Raw layer model for aic.BASE_OLIDS__PERSON
-- Source: "DATA_LAKE__NCL"."AIC_DEV"
-- Description: AIC pipelines
-- This is a 1:1 passthrough from source with standardized column names
select
    "ID" as id,
    "NHS_NUMBER_HASH" as nhs_number_hash,
    "TITLE" as title,
    "GENDER_CONCEPT_ID" as gender_concept_id,
    "BIRTH_YEAR" as birth_year,
    "BIRTH_MONTH" as birth_month,
    "DEATH_YEAR" as death_year,
    "DEATH_MONTH" as death_month
from {{ source('aic', 'BASE_OLIDS__PERSON') }}
