{{
    config(
        materialized='table',
        cluster_by=['person_id', 'clinical_effective_date'],
        tags=['smi_registry']
    )
}}
/*
MH/SMI record of alcohol consumption QOF Indicator MH007. Date of the alcohol consumption code ALC_COD
Includes ALL persons (active, inactive, deceased) following intermediate layer principles.
*/

SELECT
    obs.id,
    obs.person_id,
    obs.clinical_effective_date,
    obs.mapped_concept_code AS concept_code,
    obs.mapped_concept_display AS concept_display,
    obs.result_value,
    obs.result_unit_display

FROM ({{ get_observations("'ALC_COD'") }}) obs
WHERE obs.clinical_effective_date IS NOT NULL 