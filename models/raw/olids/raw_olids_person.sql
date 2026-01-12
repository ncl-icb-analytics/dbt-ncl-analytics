{{
    config(
        description="Raw layer (OLIDS stable layer - cleaned and filtered patient records). 1:1 passthrough with cleaned column names. \nSource: DATA_LAKE.OLIDS.PERSON \ndbt: source(''olids'', ''PERSON'') \nColumns:\n  ID -> id\n  NHS_NUMBER_HASH -> nhs_number_hash\n  TITLE -> title\n  GENDER_CONCEPT_ID -> gender_concept_id\n  BIRTH_YEAR -> birth_year\n  BIRTH_MONTH -> birth_month\n  DEATH_YEAR -> death_year\n  DEATH_MONTH -> death_month"
    )
}}
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
