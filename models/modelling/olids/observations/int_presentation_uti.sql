{{
    config(
        materialized='table',
        cluster_by=['person_id', 'clinical_effective_date'],
        tags='RADIX')
}}

/*
All acute UTIs from observations. 
Includes ALL persons (active, inactive, deceased).
*/

WITH base_observations as (
    SELECT
        obs.id,
        obs.person_id,
        obs.clinical_effective_date,
        obs.mapped_concept_code AS concept_code,
        obs.mapped_concept_display AS concept_display,
        obs.cluster_id AS source_cluster_id,
    
    FROM ({{ get_observations("'ACUTE_URINARY_TRACT_INFECTION'") }}) obs
    WHERE obs.clinical_effective_date IS NOT NULL
    AND obs.clinical_effective_date <= CURRENT_DATE() -- No future dates
)
--select all to then deduplicate by person, code and date
select person_id
    ,DATE(clinical_effective_date) AS clinical_effective_date
    ,concept_code
    ,concept_display
from base_observations
QUALIFY ROW_NUMBER() OVER (PARTITION BY PERSON_ID, CONCEPT_CODE, CLINICAL_EFFECTIVE_DATE ORDER BY PERSON_ID) = 1