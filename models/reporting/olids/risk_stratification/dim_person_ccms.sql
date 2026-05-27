{{
    config(
        materialized='table',
        cluster_by=['person_id'])
}}

/*
Published Cambridge Comorbidity Score per person.

Joins the in-house computed score (int_ccms_score) to dim_person — only
persons with at least one linked patient record appear here.

No opt-out filter is applied at this layer. For secondary use, consumers
should INNER JOIN to REPORTING.OLIDS_PERSON_STATUS.DIM_PERSON_SECONDARY_USE_ALLOWED
ON person_id per the project convention.

Replaces stg_aic_int_ccms_current. Source code definition is identical to
the AIC version (2022 modified CCMS); differences vs the AIC-published
score should be limited to OLIDS-vs-LDS staging differences and refresh
cadence.
*/

SELECT
    s.ccms_score_id,
    s.person_id,
    s.cambridge_comorbidity_score,
    s.last_updated
FROM {{ ref('int_ccms_score') }} s
INNER JOIN {{ ref('dim_person') }} p
    ON s.person_id = p.person_id
