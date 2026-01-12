{{
    config(
        description="Raw layer (Data management reference datasets). 1:1 passthrough with cleaned column names. \nSource: DATA_LAKE__NCL.DATA_MANAGEMENT.TABLE_SCHEMA_DATABASE_MONTHLY \ndbt: source(''reference_data_management'', ''TABLE_SCHEMA_DATABASE_MONTHLY'') \nColumns:\n  DATABASE_NAME -> database_name\n  TABLE_SCHEMA -> table_schema\n  TABLE_NAME -> table_name\n  ROW_COUNT -> row_count\n  COMMENT -> comment\n  LAST_DDL_BY -> last_ddl_by\n  TABLE_TYPE -> table_type\n  IS_TRANSIENT -> is_transient\n  CREATED -> created\n  LAST_ALTERED -> last_altered\n  LAST_DDL -> last_ddl\n  RUN_DATE -> run_date"
    )
}}
select
    "DATABASE_NAME" as database_name,
    "TABLE_SCHEMA" as table_schema,
    "TABLE_NAME" as table_name,
    "ROW_COUNT" as row_count,
    "COMMENT" as comment,
    "LAST_DDL_BY" as last_ddl_by,
    "TABLE_TYPE" as table_type,
    "IS_TRANSIENT" as is_transient,
    "CREATED" as created,
    "LAST_ALTERED" as last_altered,
    "LAST_DDL" as last_ddl,
    "RUN_DATE" as run_date
from {{ source('reference_data_management', 'TABLE_SCHEMA_DATABASE_MONTHLY') }}
