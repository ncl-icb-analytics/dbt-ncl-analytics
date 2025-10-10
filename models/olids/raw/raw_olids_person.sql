-- Raw layer model for olids.PERSON
-- Source: "DATA_LAKE"."OLIDS"
-- Description: OLIDS stable layer - cleaned and filtered patient records
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
from {{ source('olids', 'PERSON') }}
