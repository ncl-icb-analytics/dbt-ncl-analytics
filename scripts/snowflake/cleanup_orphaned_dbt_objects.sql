/*
    Cleanup Orphaned dbt Objects

    This procedure and task automatically drops tables/views that were created by
    dbt models which no longer exist in the project. Objects are identified by their
    comments containing both the robot emoji and the GitHub repository URL.

    Objects are dropped if:
    - Comment contains '🤖' AND 'github.com/ncl-icb-analytics/dbt-ncl-analytics'
    - Not in current dbt models (DATA_LAKE__NCL.DBT_OBSERVABILITY.DBT_MODELS)
    - Last altered > 21 days ago

    Run with role: DBT_ADMIN
    Database: ACCOUNT_MANAGEMENT
    Schema: DBT_MANAGEMENT
*/

USE ROLE DBT_ADMIN;
USE DATABASE ACCOUNT_MANAGEMENT;
USE SCHEMA DBT_MANAGEMENT;

-- Create stored procedure
CREATE OR REPLACE PROCEDURE CLEANUP_ORPHANED_DBT_OBJECTS()
RETURNS VARCHAR
LANGUAGE SQL
EXECUTE AS CALLER
AS
$$
DECLARE
    dropped_count INTEGER DEFAULT 0;
    drop_type VARCHAR;
    c CURSOR FOR
        WITH snowflake_objects AS (
            SELECT
                table_catalog,
                table_schema,
                table_name,
                table_type,
                comment,
                last_altered,
                DATEDIFF(day, last_altered, CURRENT_DATE) AS days_since_altered
            FROM SNOWFLAKE.ACCOUNT_USAGE.TABLES
            WHERE comment LIKE '%🤖%'
              AND comment LIKE '%github.com/ncl-icb-analytics/dbt-ncl-analytics%'
              AND table_catalog IN (
                'MODELLING',
                'REPORTING',
                'PUBLISHED_REPORTING__DIRECT_CARE',
                'PUBLISHED_REPORTING__SECONDARY_USE',
                'DEV__MODELLING',
                'DEV__REPORTING',
                'DEV__PUBLISHED_REPORTING__DIRECT_CARE',
                'DEV__PUBLISHED_REPORTING__SECONDARY_USE'
              )
              AND deleted IS NULL
        ),
        current_dbt_models AS (
            SELECT
                UPPER(database_name) AS database_name,
                UPPER(schema_name) AS schema_name,
                UPPER(alias) AS table_name
            FROM DATA_LAKE__NCL.DBT_OBSERVABILITY.DBT_MODELS
            WHERE package_name = 'ncl_analytics'
        )
        SELECT
            s.table_catalog,
            s.table_schema,
            s.table_name,
            s.table_type
        FROM snowflake_objects s
        LEFT JOIN current_dbt_models m
            ON s.table_catalog = m.database_name
           AND s.table_schema = m.schema_name
           AND s.table_name = m.table_name
        WHERE m.table_name IS NULL
          AND s.days_since_altered > 21;
BEGIN
    FOR rec IN c DO
        drop_type := CASE rec.table_type WHEN 'BASE TABLE' THEN 'TABLE' ELSE rec.table_type END;
        EXECUTE IMMEDIATE 'DROP ' || drop_type || ' IF EXISTS ' || rec.table_catalog || '.' || rec.table_schema || '.' || rec.table_name;
        dropped_count := dropped_count + 1;
    END FOR;
    RETURN 'Dropped ' || dropped_count || ' orphaned dbt objects';
END;
$$;

-- Create scheduled task (runs weekly on Sunday at 6am UK time)
CREATE OR REPLACE TASK TASK_CLEANUP_ORPHANED_DBT_OBJECTS
    WAREHOUSE = WH_NCL_ENGINEERING_XS
    SCHEDULE = 'USING CRON 0 6 * * 0 Europe/London'
AS
    CALL CLEANUP_ORPHANED_DBT_OBJECTS();

-- Enable the task
ALTER TASK TASK_CLEANUP_ORPHANED_DBT_OBJECTS RESUME;


CALL CLEANUP_ORPHANED_DBT_OBJECTS();
