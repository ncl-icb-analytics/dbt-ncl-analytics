{{ config(
    materialized='table') }}

-- CYP_AST_61 case finding: Children and young people with asthma symptoms but no formal diagnosis
-- Identifies children (18 months to under 18 years) with asthma symptoms who need formal diagnosis
-- Based on legacy logic: medications OR observations in last 12 months, no formal diagnosis

WITH cyp_base_population AS (
    -- Children and young people aged 18 months to under 18 years
    SELECT
        base.person_id,
        age.age,
        age.age_days_approx
    FROM {{ ref('int_ltc_lcs_cf_base_population') }} AS base
    INNER JOIN
        {{ ref('dim_person_age') }} AS age
        ON base.person_id = age.person_id
    WHERE
        age.age_days_approx >= 547  -- 18 months
        AND age.age < 18  -- under 18 years
),

asthma_diagnosis AS (
    -- Patients with formal asthma diagnosis (excluding resolved asthma as latest)
    SELECT DISTINCT person_id
    FROM {{ ref('int_ltc_lcs_cyp_asthma_observations') }}
    WHERE cluster_id IN ('ASTHMA_DIAGNOSIS', 'ASTHMA_RESOLVED')
    QUALIFY ROW_NUMBER() OVER (PARTITION BY person_id ORDER BY clinical_effective_date DESC) = 1
        AND cluster_id != 'ASTHMA_RESOLVED'  -- Exclude those with resolved asthma as latest
),

asthma_symptoms AS (
    -- Get patients with asthma symptoms in last 12 months (medications OR observations)
    SELECT
        person_id,
        MAX(clinical_effective_date) AS latest_symptom_date
    FROM (
        -- Asthma medications in last 12 months
        SELECT
            person_id,
            order_date AS clinical_effective_date
        FROM {{ ref('int_ltc_lcs_cyp_asthma_medications') }}
        WHERE
            cluster_id IN ('ASTHMA_MEDICATIONS', 'ASTHMA_PREDNISOLONE', 'MONTELUKAST_MEDICATIONS')
            AND order_date >= DATEADD(MONTH, -12, CURRENT_DATE())

        UNION ALL

        -- Suspected asthma observations in last 12 months (all ages 18m-17y)
        SELECT
            person_id,
            clinical_effective_date
        FROM {{ ref('int_ltc_lcs_cyp_asthma_observations') }}
        WHERE
            cluster_id = 'SUSPECTED_ASTHMA'
            AND clinical_effective_date >= DATEADD(MONTH, -12, CURRENT_DATE())
    )
    GROUP BY person_id
),

viral_wheeze_age_6_plus AS (
    -- Viral induced wheeze in last 12 months (age ≥6 years only)
    SELECT
        obs.person_id,
        MAX(obs.clinical_effective_date) AS latest_viral_wheeze_date
    FROM {{ ref('int_ltc_lcs_cyp_asthma_observations') }} AS obs
    INNER JOIN {{ ref('dim_person_age') }} AS age
        ON obs.person_id = age.person_id
    WHERE
        obs.cluster_id = 'VIRAL_WHEEZE'
        AND obs.clinical_effective_date >= DATEADD(MONTH, -12, CURRENT_DATE())
        AND age.age >= 6
    GROUP BY obs.person_id
)

-- Final selection: CYP with asthma symptoms but no formal diagnosis
SELECT
    bp.person_id,
    bp.age,
    CASE
        WHEN symptoms.latest_symptom_date IS NOT NULL
            OR vw.latest_viral_wheeze_date IS NOT NULL THEN TRUE
        ELSE FALSE
    END AS has_asthma_symptoms,
    GREATEST(
        COALESCE(symptoms.latest_symptom_date, '1900-01-01'::DATE),
        COALESCE(vw.latest_viral_wheeze_date, '1900-01-01'::DATE)
    ) AS latest_symptom_date
FROM cyp_base_population AS bp
LEFT JOIN asthma_symptoms AS symptoms ON bp.person_id = symptoms.person_id
LEFT JOIN viral_wheeze_age_6_plus AS vw ON bp.person_id = vw.person_id
WHERE
    (symptoms.person_id IS NOT NULL OR vw.person_id IS NOT NULL)
    AND NOT EXISTS (
        SELECT 1 FROM asthma_diagnosis AS ad
        WHERE ad.person_id = bp.person_id
    )
