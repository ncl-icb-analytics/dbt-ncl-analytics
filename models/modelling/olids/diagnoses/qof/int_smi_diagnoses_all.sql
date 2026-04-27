{{
    config(
        materialized='table',
        cluster_by=['person_id', 'clinical_effective_date'])
}}

/*
All serious mental illness (SMI) diagnosis observations from clinical records.
Uses MH_COD cluster for mental health diagnoses (schizophrenia, bipolar disorder, other psychoses).

Per QOF MH001 spec, MH1_REG is "ever diagnosed" with no remission exclusion.
MHREM_COD is not used — the register includes all patients with MH_DAT != Null.
8.8.2026 KH SMI health checks core or enhanced are only performed on SMI patients not in remission so we need to keep resolved status logic in
Use group by to aggregate records where identical clinical_effective_date, person_id and concept_code appear twice because they are in both MH_COD and MHREM_COD.
*/
With base_observations as (
SELECT 
    obs.id,
    obs.person_id,
    obs.clinical_effective_date,
    obs.mapped_concept_code AS concept_code,
    obs.mapped_concept_display AS concept_display,
   CASE
    WHEN MAX(CASE WHEN obs.cluster_id = 'MHREM_COD' THEN 1 ELSE 0 END) = 1
        THEN 'SMI Resolved'
    ELSE 'SMI Diagnosis'
    END AS smi_observation_type,
 -- SMI-specific flags (observation-level only)
 /* Coalesce cluster with MHREM_COD taking precedence*/
    CASE
        WHEN MAX(CASE WHEN obs.cluster_id = 'MHREM_COD' THEN 1 ELSE 0 END) = 1
            THEN 'MHREM_COD'
        WHEN MAX(CASE WHEN obs.cluster_id = 'MH_COD' THEN 1 ELSE 0 END) = 1
            THEN 'MH_COD'
        ELSE NULL
    END AS source_cluster_id,
    /* Diagnosis is only true if it exists AND resolved does NOT exist */
    CASE
        WHEN MAX(CASE WHEN obs.cluster_id = 'MHREM_COD' THEN 1 ELSE 0 END) = 1
            THEN FALSE
        WHEN MAX(CASE WHEN obs.cluster_id = 'MH_COD' THEN 1 ELSE 0 END) = 1
            THEN TRUE
        ELSE FALSE
    END AS is_diagnosis_code,

    /* Resolved wins if present */
    CASE
        WHEN MAX(CASE WHEN obs.cluster_id = 'MHREM_COD' THEN 1 ELSE 0 END) = 1
            THEN TRUE
        ELSE FALSE
    END AS is_resolved_code
    
FROM ({{ get_observations("'MH_COD','MHREM_COD'", source='PCD') }}) obs
GROUP BY ALL
ORDER BY person_id, clinical_effective_date, id
)
SELECT *
FROM base_observations
QUALIFY ROW_NUMBER() OVER (PARTITION BY PERSON_ID, CONCEPT_CODE, CLINICAL_EFFECTIVE_DATE ORDER BY PERSON_ID) = 1
