{{
    config(
        materialized='table',
        cluster_by=['person_id', 'clinical_effective_date'],
        tags=['smi_registry']
        )
}}
--using mixed codes defined by SMI Longer Lives Campaign covers exercise habits.

with NUTR as (
SELECT
    obs.id,
    obs.person_id,
    obs.clinical_effective_date,
    obs.mapped_concept_code AS concept_code,
    obs.mapped_concept_display AS concept_display,
  FROM ({{ get_observations("'SMI_LONGER_LIVES_EXERCISE_ASSESSMENT'", source='ECL_CACHE') }}) obs
WHERE obs.clinical_effective_date IS NOT NULL 
AND obs.clinical_effective_date <= CURRENT_DATE() -- No future dates
)
--select all to then deduplicate by person, code and date
select person_id
,clinical_effective_date
,concept_code
,concept_display
,CASE 
WHEN concept_code IN ('228445002','160631001') THEN 'Yes' 
WHEN concept_code IN ('160632008','160633003') THEN 'No'
WHEN concept_code IN ('160628002','266930008') THEN 'Unclear'
WHEN concept_code = '160629005' THEN 'Exercise physically impossible'
END AS low_exercise_flag
from NUTR
QUALIFY ROW_NUMBER() OVER (PARTITION BY PERSON_ID, CONCEPT_CODE, CLINICAL_EFFECTIVE_DATE ORDER BY PERSON_ID) = 1