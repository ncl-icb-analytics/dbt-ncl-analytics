{{
    config(
       materialized='table',
        cluster_by=['person_id', 'clinical_effective_date'],
        tags=['smi_registry'])
}}
 --This model captures observations where Cholesterol measurement was declined by the patient.
 --This table is empty if there are no such observations in the source data. Maybe the concepts 
SELECT
    obs.id,
    obs.person_id,
    obs.clinical_effective_date,
    obs.mapped_concept_code AS concept_code,
    obs.mapped_concept_display AS concept_display,

FROM ({{ get_observations ("'MHPCADEC_COD', 'MHPCAPU_COD'") }}) obs
WHERE obs.clinical_effective_date IS NOT NULL 
AND obs.clinical_effective_date <= CURRENT_DATE() -- No future dates