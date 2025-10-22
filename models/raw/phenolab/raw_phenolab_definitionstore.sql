-- Raw layer model for phenolab.DEFINITIONSTORE
-- Source: "DATA_LAKE__NCL"."PHENOLAB_DEV"
-- Description: Phenolab supporting data
-- This is a 1:1 passthrough from source with standardized column names
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
from {{ source('phenolab', 'DEFINITIONSTORE') }}
