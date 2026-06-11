/*
    Drop legacy raw/staging schemas from MODELLING

    Raw and staging models moved to the STAGING database (DBT_RAW + one schema
    per source system). These schemas only ever fed dbt models, so they are
    safe to drop once a full prod and dev build has completed against STAGING.

    Run as ENGINEER (or any role owning MODELLING). One-off; the
    CLEANUP_ORPHANED_DBT_OBJECTS task would otherwise remove the objects
    after 21 days but leave the empty schemas behind.
*/

DROP SCHEMA IF EXISTS MODELLING.DBT_RAW;
DROP SCHEMA IF EXISTS MODELLING.DBT_STAGING;
DROP SCHEMA IF EXISTS DEV__MODELLING.DBT_RAW;
DROP SCHEMA IF EXISTS DEV__MODELLING.DBT_STAGING;

-- stg_mhcorl previously used an explicit schema override into MODELLING.MH.
-- That schema also holds analyst-managed (non-dbt) objects, so drop only the
-- dbt table, not the schema.
DROP TABLE IF EXISTS MODELLING.MH.STG_MHCORL;
DROP TABLE IF EXISTS DEV__MODELLING.MH.STG_MHCORL;

-- Three cancer staging views were co-located with the analyst-managed
-- CANCER__REF schema via a schema override; they now build to
-- STAGING.REFERENCE. Drop only the dbt views; the schema stays.
DROP VIEW IF EXISTS MODELLING.CANCER__REF.STG_REFERENCE_CANCER_LSOA_TO_CANCER_ALLIANCE;
DROP VIEW IF EXISTS MODELLING.CANCER__REF.STG_REFERENCE_CANCER_PROVIDER_TO_CANCER_ALLIANCE;
DROP VIEW IF EXISTS MODELLING.CANCER__REF.STG_REFERENCE_CANCER_SUB_ICB_TO_CANCER_ALLIANCE;
DROP VIEW IF EXISTS DEV__MODELLING.CANCER__REF.STG_REFERENCE_CANCER_LSOA_TO_CANCER_ALLIANCE;
DROP VIEW IF EXISTS DEV__MODELLING.CANCER__REF.STG_REFERENCE_CANCER_PROVIDER_TO_CANCER_ALLIANCE;
DROP VIEW IF EXISTS DEV__MODELLING.CANCER__REF.STG_REFERENCE_CANCER_SUB_ICB_TO_CANCER_ALLIANCE;
