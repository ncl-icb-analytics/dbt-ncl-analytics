{{
    config(
        materialized='table',
        cluster_by=['person_id'])
}}

/*
Base population for the GP BP Registry research cohort (QMUL).

Inclusion rules applied here:
  - Adult (age >= 18)
  - Active hypertension diagnosis: latest HYP_COD after any HYPRES_COD, or HYP_COD only
  - At least one oral antihypertensive medication order on record

Further inclusion criteria (BP reading count and spacing) are applied in downstream
models. Exclusion of readings taken during pregnancy/HDP windows is applied separately
to BP events rather than removing patients here.

One row per eligible person.
*/

WITH adults AS (

    SELECT person_id, age
    FROM {{ ref('dim_person_age') }}
    WHERE age >= 18

),

htn_diagnoses AS (

    SELECT
        person_id,
        MAX(
            CASE WHEN is_diagnosis_code THEN clinical_effective_date END
        ) AS latest_diagnosis_date,
        MAX(
            CASE WHEN is_resolved_code THEN clinical_effective_date END
        ) AS latest_resolved_date,
        MIN(
            CASE WHEN is_diagnosis_code THEN clinical_effective_date END
        ) AS earliest_diagnosis_date
    FROM {{ ref('int_hypertension_diagnoses_all') }}
    GROUP BY person_id

),

active_htn AS (

    SELECT
        person_id,
        earliest_diagnosis_date,
        latest_diagnosis_date
    FROM htn_diagnoses
    WHERE latest_diagnosis_date IS NOT NULL
      AND (
          latest_resolved_date IS NULL
          OR latest_diagnosis_date > latest_resolved_date
      )

),

antihyp_orders AS (

    SELECT
        person_id,
        MIN(order_date) AS earliest_antihyp_order_date,
        MAX(order_date) AS latest_antihyp_order_date
    FROM {{ ref('int_antihypertensive_medications_all') }}
    GROUP BY person_id

)

SELECT
    a.person_id,
    a.age,
    h.earliest_diagnosis_date,
    h.latest_diagnosis_date,
    m.earliest_antihyp_order_date,
    m.latest_antihyp_order_date

FROM adults a
INNER JOIN active_htn h ON a.person_id = h.person_id
INNER JOIN antihyp_orders m ON a.person_id = m.person_id
