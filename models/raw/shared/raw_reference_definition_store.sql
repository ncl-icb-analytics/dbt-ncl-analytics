{{
    config(
        description="Raw layer (Reference terminology data including SNOMED, BNF, and other code sets). 1:1 passthrough with cleaned column names. \nSource: DATA_LAKE__NCL.TERMINOLOGY.DEFINITION_STORE \ndbt: source(''reference_terminology'', ''DEFINITION_STORE'') \nColumns:\n  CODE -> code\n  CODE_DESCRIPTION -> code_description\n  VOCABULARY -> vocabulary\n  CODELIST_ID -> codelist_id\n  CODELIST_NAME -> codelist_name\n  CODELIST_VERSION -> codelist_version\n  DEFINITION_ID -> definition_id\n  DEFINITION_NAME -> definition_name\n  DEFINITION_VERSION -> definition_version\n  DEFINITION_SOURCE -> definition_source\n  VERSION_DATETIME -> version_datetime\n  UPLOADED_DATETIME -> uploaded_datetime\n  SOURCE_TABLE -> source_table\n  DBID -> dbid\n  CORE_CONCEPT_ID -> core_concept_id"
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
    "UPLOADED_DATETIME" as uploaded_datetime,
    "SOURCE_TABLE" as source_table,
    "DBID" as dbid,
    "CORE_CONCEPT_ID" as core_concept_id
from {{ source('reference_terminology', 'DEFINITION_STORE') }}
