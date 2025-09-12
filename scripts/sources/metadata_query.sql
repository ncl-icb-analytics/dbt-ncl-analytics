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
  
    -- eRS_primary_care: Primary care referrals data
  SELECT 
    'DATA_LAKE' as database_name,
    'ERS' as schema_name,
    table_name,
    column_name,
    data_type,
    ordinal_position
  FROM "DATA_LAKE".INFORMATION_SCHEMA.COLUMNS
  WHERE table_schema = 'ERS'
  
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
  
    -- dictionary_dbo: Reference data including PDS and lookup tables
  SELECT 
    'Dictionary' as database_name,
    'dbo' as schema_name,
    table_name,
    column_name,
    data_type,
    ordinal_position
  FROM "Dictionary".INFORMATION_SCHEMA.COLUMNS
  WHERE table_schema = 'dbo'
  
  UNION ALL
  
    -- dictionary_ecds: Reference data for ECDS
  SELECT 
    'Dictionary' as database_name,
    'ECDS_ETOS' as schema_name,
    table_name,
    column_name,
    data_type,
    ordinal_position
  FROM "Dictionary".INFORMATION_SCHEMA.COLUMNS
  WHERE table_schema = 'ECDS_ETOS'
  
  UNION ALL
  
    -- dictionary_op: Reference data for outpatient procedures and treatments
  SELECT 
    'Dictionary' as database_name,
    'OP' as schema_name,
    table_name,
    column_name,
    data_type,
    ordinal_position
  FROM "Dictionary".INFORMATION_SCHEMA.COLUMNS
  WHERE table_schema = 'OP'
  
  UNION ALL
  
    -- dictionary_ip: Reference data for inpatient procedures and treatments
  SELECT 
    'Dictionary' as database_name,
    'IP' as schema_name,
    table_name,
    column_name,
    data_type,
    ordinal_position
  FROM "Dictionary".INFORMATION_SCHEMA.COLUMNS
  WHERE table_schema = 'IP'
  
  UNION ALL
  
    -- dictionary_snomed: Reference data for snomed
  SELECT 
    'Dictionary' as database_name,
    'Snomed' as schema_name,
    table_name,
    column_name,
    data_type,
    ordinal_position
  FROM "Dictionary".INFORMATION_SCHEMA.COLUMNS
  WHERE table_schema = 'Snomed'
  
  UNION ALL
  
    -- dictionary_eRS: Primary care referrals lookups
  SELECT 
    'Dictionary' as database_name,
    'E-Referral' as schema_name,
    table_name,
    column_name,
    data_type,
    ordinal_position
  FROM "Dictionary".INFORMATION_SCHEMA.COLUMNS
  WHERE table_schema = 'E-Referral'
  
  UNION ALL
  
    -- csds: Community services dataset
  SELECT 
    'DATA_LAKE' as database_name,
    'CSDS' as schema_name,
    table_name,
    column_name,
    data_type,
    ordinal_position
  FROM "DATA_LAKE".INFORMATION_SCHEMA.COLUMNS
  WHERE table_schema = 'CSDS'
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