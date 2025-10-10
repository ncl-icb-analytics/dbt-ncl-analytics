-- Schema comparison between MODELLING.LOOKUP_NCL and DEV__MODELLING.LOOKUP_NCL
-- Compares object existence across deployment targets using metadata only
-- Does not query actual row counts, so works even with broken views
--
-- Usage: Run directly in Snowflake (no dbt compile needed)

WITH modelling_tables AS (
    SELECT
        table_name,
        table_type,
        row_count,
        created as created_date,
        last_altered as last_altered_date
    FROM MODELLING.information_schema.tables
    WHERE table_schema = 'LOOKUP_NCL'
        AND table_type IN ('BASE TABLE', 'VIEW')
),

dev_tables AS (
    SELECT
        table_name,
        table_type,
        row_count,
        created as created_date,
        last_altered as last_altered_date
    FROM DEV__MODELLING.information_schema.tables
    WHERE table_schema = 'LOOKUP_NCL'
        AND table_type IN ('BASE TABLE', 'VIEW')
),

all_tables AS (
    SELECT table_name FROM modelling_tables
    UNION
    SELECT table_name FROM dev_tables
)

SELECT
    at.table_name,
    COALESCE(mt.table_type, dt.table_type) as object_type,
    CASE WHEN mt.table_name IS NOT NULL THEN '✓' ELSE '✗' END as in_modelling,
    CASE WHEN dt.table_name IS NOT NULL THEN '✓' ELSE '✗' END as in_dev,
    CASE
        WHEN mt.table_name IS NOT NULL AND dt.table_name IS NULL THEN '❌ MISSING IN DEV'
        WHEN mt.table_name IS NULL AND dt.table_name IS NOT NULL THEN '📝 DEV ONLY'
        WHEN mt.table_type != dt.table_type THEN '⚠️ TYPE MISMATCH'
        WHEN mt.table_type = 'VIEW' THEN '✅ EXISTS IN BOTH'
        WHEN mt.row_count = dt.row_count THEN '✅ IDENTICAL'
        WHEN ABS(mt.row_count - dt.row_count) > (mt.row_count * 0.1) THEN '⚠️ LARGE DIFFERENCE'
        ELSE '⚠️ MINOR DIFFERENCE'
    END as status
FROM all_tables at
LEFT JOIN modelling_tables mt ON at.table_name = mt.table_name
LEFT JOIN dev_tables dt ON at.table_name = dt.table_name
ORDER BY
    CASE
        WHEN mt.table_name IS NOT NULL AND dt.table_name IS NULL THEN 0  -- Missing in dev first (critical)
        WHEN mt.table_name IS NULL AND dt.table_name IS NOT NULL THEN 2  -- Dev only last (expected)
        ELSE 1                                                            -- Existing in both in middle
    END,
    at.table_name
