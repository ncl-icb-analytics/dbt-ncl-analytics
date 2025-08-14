-- Dynamically generated metadata query from source_mappings.yml
-- Query all databases and schemas defined in your source configuration
--
-- Usage:
--   1. Copy this entire query
--   2. Paste into Snowflake UI
--   3. Execute query
--   4. Export results as CSV to table_metadata.csv

WITH schema_metadata AS (
    -- wl: Waiting lists and patient pathway data
  SELECT 
    'DATA_LAKE' as database_name,
    'WL' as schema_name,
    table_name,
    column_name,
    data_type,
    ordinal_position
  FROM "DATA_LAKE".INFORMATION_SCHEMA.COLUMNS
  WHERE table_schema = 'WL'
  
  UNION ALL
  
    -- sus_op: SUS outpatient appointments and activity
  SELECT 
    'DATA_LAKE' as database_name,
    'SUS_UNIFIED_OP' as schema_name,
    table_name,
    column_name,
    data_type,
    ordinal_position
  FROM "DATA_LAKE".INFORMATION_SCHEMA.COLUMNS
  WHERE table_schema = 'SUS_UNIFIED_OP'
  
  UNION ALL
  
    -- sus_apc: SUS admitted patient care episodes and procedures
  SELECT 
    'DATA_LAKE' as database_name,
    'SUS_UNIFIED_APC' as schema_name,
    table_name,
    column_name,
    data_type,
    ordinal_position
  FROM "DATA_LAKE".INFORMATION_SCHEMA.COLUMNS
  WHERE table_schema = 'SUS_UNIFIED_APC'
  
  UNION ALL
  
    -- sus_ae: SUS emergency care attendances and activity
  SELECT 
    'DATA_LAKE' as database_name,
    'SUS_UNIFIED_ECDS' as schema_name,
    table_name,
    column_name,
    data_type,
    ordinal_position
  FROM "DATA_LAKE".INFORMATION_SCHEMA.COLUMNS
  WHERE table_schema = 'SUS_UNIFIED_ECDS'
  
  UNION ALL
  
    -- epd_primary_care: Primary care medications and prescribing data
  SELECT 
    'DATA_LAKE' as database_name,
    'EPD_PRIMARY_CARE' as schema_name,
    table_name,
    column_name,
    data_type,
    ordinal_position
  FROM "DATA_LAKE".INFORMATION_SCHEMA.COLUMNS
  WHERE table_schema = 'EPD_PRIMARY_CARE'
  
  UNION ALL
  
    -- dictionary: Reference data including PDS and lookup tables
  SELECT 
    'Dictionary' as database_name,
    'dbo' as schema_name,
    table_name,
    column_name,
    data_type,
    ordinal_position
  FROM "Dictionary".INFORMATION_SCHEMA.COLUMNS
  WHERE table_schema = 'dbo'
)

SELECT 
  database_name as "DATABASE_NAME",
  schema_name as "SCHEMA_NAME", 
  table_name as "TABLE_NAME",
  column_name as "COLUMN_NAME",
  data_type as "DATA_TYPE",
  ordinal_position as "ORDINAL_POSITION"
FROM schema_metadata
ORDER BY database_name, schema_name, table_name, ordinal_position;