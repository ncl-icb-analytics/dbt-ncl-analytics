-- Raw layer model for reference_data_management.TABLE_SCHEMA_DATABASE
-- Source: "DATA_LAKE__NCL"."DATA_MANAGEMENT"
-- Description: Data management reference datasets
-- This is a 1:1 passthrough from source with standardized column names
select
    "DATABASE_NAME" as database_name,
    "TABLE_SCHEMA" as table_schema,
    "TABLE_NAME" as table_name,
    "ROW_COUNT" as row_count,
    "COMMENT" as comment,
    "LAST_DDL_BY" as last_ddl_by,
    "TABLE_TYPE" as table_type,
    "CREATED" as created,
    "LAST_ALTERED" as last_altered,
    "LAST_DDL" as last_ddl
from {{ source('reference_data_management', 'TABLE_SCHEMA_DATABASE') }}
