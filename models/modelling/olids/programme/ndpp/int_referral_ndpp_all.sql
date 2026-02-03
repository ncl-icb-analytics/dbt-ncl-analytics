{{
    config(
        materialized='table',
        cluster_by=['person_id', 'clinical_effective_date'],
        tags=['smi_registry']
        )
}}
--using PCDREFSET CODES FOR NDPP REFERRALS including invitations sent and declines

with ndpp AS (
SELECT
    obs.id,
    obs.person_id,
    obs.clinical_effective_date,
    obs.mapped_concept_code AS concept_code,
    obs.mapped_concept_display AS concept_display,
    FROM ({{ get_observations("'DPPOFF_COD'") }}) obs
WHERE obs.clinical_effective_date IS NOT NULL 
AND obs.clinical_effective_date <= CURRENT_DATE() -- No future dates
)
,dedup_ndpp AS (
--select all to then deduplicate by person, code and date
select person_id
,DATE(clinical_effective_date) AS clinical_effective_date
,concept_code
,concept_display
from ndpp 
QUALIFY ROW_NUMBER() OVER (PARTITION BY PERSON_ID, CONCEPT_CODE, CLINICAL_EFFECTIVE_DATE ORDER BY PERSON_ID) = 1
)
-- prioritising declines over invitations where both exist on the same date
select *
from dedup_ndpp
QUALIFY ROW_NUMBER() OVER (PARTITION BY PERSON_ID, CLINICAL_EFFECTIVE_DATE
        ORDER BY CASE
            WHEN CONCEPT_DISPLAY = 'NHS Diabetes Prevention Programme invitation' THEN 0
            WHEN CONCEPT_DISPLAY = 'Referral to NHS Diabetes Prevention Programme declined' THEN 1
            ELSE -1
            END DESC) = 1