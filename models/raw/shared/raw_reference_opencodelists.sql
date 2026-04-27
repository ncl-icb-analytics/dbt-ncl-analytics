{{
    config(
        description="Raw layer (Reference terminology data including SNOMED, BNF, and other code sets). 1:1 passthrough with cleaned column names. \nSource: DATA_LAKE__NCL.TERMINOLOGY.OPENCODELISTS \ndbt: source(''reference_terminology'', ''OPENCODELISTS'') \nColumns:\n  CODELIST_ORG -> codelist_org\n  CODELIST_SLUG -> codelist_slug\n  CODELIST_NAME -> codelist_name\n  VERSION_HASH -> version_hash\n  CODING_SYSTEM -> coding_system\n  CODE -> code\n  TERM -> term\n  CLUSTER_ID -> cluster_id"
    )
}}
select
    "CODELIST_ORG" as codelist_org,
    "CODELIST_SLUG" as codelist_slug,
    "CODELIST_NAME" as codelist_name,
    "VERSION_HASH" as version_hash,
    "CODING_SYSTEM" as coding_system,
    "CODE" as code,
    "TERM" as term,
    "CLUSTER_ID" as cluster_id
from {{ source('reference_terminology', 'OPENCODELISTS') }}
