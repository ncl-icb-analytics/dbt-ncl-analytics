{{
    config(
        materialized='table',
        cluster_by=['person_id', 'clinical_effective_date'],
        tags=['smi_registry']
        )
}}
--using all 3 prompts defined by SMI Enhanced health checks for Cancer Screening campaign and filter to Breast Screening prompts only
WITH ALL_SCREENING_PROMPTS as (
SELECT
    obs.id,
    obs.person_id,
    obs.clinical_effective_date,
    obs.mapped_concept_code AS concept_code,
    obs.mapped_concept_display AS concept_display,
FROM ({{ get_observations("'SMI_LONGER_LIVES_CANCER_SCREEN_PROMPTS'", source='ECL_CACHE') }}) obs
WHERE obs.clinical_effective_date IS NOT NULL 
AND obs.clinical_effective_date <= CURRENT_DATE() -- No future dates
)
--select all to then deduplicate by person, code and date
select person_id
,clinical_effective_date
,concept_code
,concept_display
from ALL_SCREENING_PROMPTS
WHERE concept_code = '710871000000104' -- SNOMED code for Breast Screening Prompt
QUALIFY ROW_NUMBER() OVER (PARTITION BY PERSON_ID, CONCEPT_CODE, CLINICAL_EFFECTIVE_DATE ORDER BY PERSON_ID) = 1