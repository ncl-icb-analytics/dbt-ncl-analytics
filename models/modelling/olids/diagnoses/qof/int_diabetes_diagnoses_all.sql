{{
    config(
        materialized='table',
        cluster_by=['person_id', 'clinical_effective_date'])
}}

/*
All diabetes diagnosis observations from clinical records.
Uses QOF diabetes cluster IDs with clinical prioritization:
- DMTYPE1_COD: Type 1 diabetes specific diagnoses (highest priority)
- DMTYPE2_COD: Type 2 diabetes specific diagnoses
- DM_COD: General diabetes diagnoses
- DMRES_COD: Diabetes resolved/remission codes (lowest priority)

Clinical Purpose:
- QOF diabetes register data collection
- Diabetes type classification support
- Disease progression tracking
- Resolution status monitoring

Includes ALL persons (active, inactive, deceased) following intermediate layer principles.
This is OBSERVATION-LEVEL data - one row per diabetes observation.
When an observation belongs to multiple clusters, we flag ALL applicable categories rather than just the most specific.
Use this model as input for fct_person_diabetes_register.sql which applies person-level aggregation and QOF business rules.
*/

WITH diabetes_observations_all_clusters AS (
    -- Get all diabetes observations with all their cluster relationships
    SELECT
        obs.id,
        obs.person_id,
        obs.clinical_effective_date,
        obs.mapped_concept_code AS concept_code,
        obs.mapped_concept_display AS concept_display,
        obs.cluster_id AS source_cluster_id
    FROM ({{ get_observations("'DM_COD', 'DMTYPE1_COD', 'DMTYPE2_COD', 'DMRES_COD'", source='PCD') }}) obs
),

diabetes_observations_categorised AS (
    -- Flag ALL applicable categories per observation (not just highest priority)
    SELECT
        d.id,
        d.person_id,
        d.clinical_effective_date,
        d.concept_code,
        d.concept_display,

        -- Flag all applicable diabetes categories for each observation
        MAX(CASE WHEN d.source_cluster_id = 'DM_COD' THEN TRUE ELSE FALSE END) AS is_general_diabetes_code,
        MAX(CASE WHEN d.source_cluster_id = 'DMTYPE1_COD' THEN TRUE ELSE FALSE END) AS is_type1_diabetes_code,
        MAX(CASE WHEN d.source_cluster_id = 'DMTYPE2_COD' THEN TRUE ELSE FALSE END) AS is_type2_diabetes_code,
        MAX(CASE WHEN d.source_cluster_id = 'DMRES_COD' THEN TRUE ELSE FALSE END) AS is_diabetes_resolved_code,

        -- Composite flags for unified clinical tracking
        MAX(CASE WHEN d.source_cluster_id IN ('DM_COD', 'DMTYPE1_COD', 'DMTYPE2_COD') THEN TRUE ELSE FALSE END) AS is_diagnosis_code,
        MAX(CASE WHEN d.source_cluster_id = 'DMRES_COD' THEN TRUE ELSE FALSE END) AS is_resolved_code,

        -- For traceability, keep the most specific cluster as source_cluster_id
        CASE
            WHEN MAX(CASE WHEN d.source_cluster_id = 'DMTYPE1_COD' THEN 1 ELSE 0 END) = 1 THEN 'DMTYPE1_COD'
            WHEN MAX(CASE WHEN d.source_cluster_id = 'DMTYPE2_COD' THEN 1 ELSE 0 END) = 1 THEN 'DMTYPE2_COD'
            WHEN MAX(CASE WHEN d.source_cluster_id = 'DM_COD' THEN 1 ELSE 0 END) = 1 THEN 'DM_COD'
            WHEN MAX(CASE WHEN d.source_cluster_id = 'DMRES_COD' THEN 1 ELSE 0 END) = 1 THEN 'DMRES_COD'
            ELSE 'UNKNOWN'
        END AS source_cluster_id
    FROM diabetes_observations_all_clusters d
    GROUP BY d.id, d.person_id, d.clinical_effective_date, d.concept_code, d.concept_display
)

SELECT
    c.id,
    c.person_id,
    c.clinical_effective_date,
    c.concept_code,
    c.concept_display,
    c.source_cluster_id,

    -- Flag different types of diabetes codes following QOF definitions
    -- Now using the cumulative flags from the categorised CTE
    c.is_general_diabetes_code,
    c.is_type1_diabetes_code,
    c.is_type2_diabetes_code,
    c.is_diabetes_resolved_code,
    c.is_diagnosis_code,
    c.is_resolved_code,

    -- Diabetes type determination (for individual observation context)
    CASE
        WHEN c.source_cluster_id = 'DMTYPE1_COD' THEN 'Type 1'
        WHEN c.source_cluster_id = 'DMTYPE2_COD' THEN 'Type 2'
        WHEN c.source_cluster_id = 'DM_COD' THEN 'General'
        WHEN c.source_cluster_id = 'DMRES_COD' THEN 'Resolved'
        ELSE 'Unknown'
    END AS diabetes_observation_type

FROM diabetes_observations_categorised c
ORDER BY c.person_id, c.clinical_effective_date, c.id
