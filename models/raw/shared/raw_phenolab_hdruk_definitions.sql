{{
    config(
        description="Raw layer (Phenolab supporting data). 1:1 passthrough with cleaned column names. \nSource: DATA_LAKE__NCL.PHENOLAB_DEV.HDRUK_DEFINITIONS \ndbt: source(''phenolab'', ''HDRUK_DEFINITIONS'') \nColumns:\n  CODE -> code\n  CODE_DESCRIPTION -> code_description\n  VOCABULARY -> vocabulary\n  CODELIST_ID -> codelist_id\n  CODELIST_NAME -> codelist_name\n  CODELIST_VERSION -> codelist_version\n  DEFINITION_ID -> definition_id\n  DEFINITION_NAME -> definition_name\n  DEFINITION_VERSION -> definition_version\n  DEFINITION_SOURCE -> definition_source\n  VERSION_DATETIME -> version_datetime\n  UPLOADED_DATETIME -> uploaded_datetime"
    )
}}
select
    "CODE" as code,
    "CODE_DESCRIPTION" as code_description,
    "VOCABULARY" as vocabulary,
    "CODELIST_ID" as codelist_id,
    "CODELIST_NAME" as codelist_name,
    "CODELIST_VERSION" as codelist_version,
    "DEFINITION_ID" as definition_id,
    "DEFINITION_NAME" as definition_name,
    "DEFINITION_VERSION" as definition_version,
    "DEFINITION_SOURCE" as definition_source,
    "VERSION_DATETIME" as version_datetime,
    "UPLOADED_DATETIME" as uploaded_datetime
from {{ source('phenolab', 'HDRUK_DEFINITIONS') }}
