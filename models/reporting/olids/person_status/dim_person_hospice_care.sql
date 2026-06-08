{{
    config(
        materialized='table',
        tags=['dimension', 'person', 'hospice', 'care'],
        cluster_by=['person_id'])
}}

-- Person Hospice Care Dimension Table
-- Persons under the care of a hospice service: full clinical care (HOSPICE_FULLCARE_COD)
-- or hospice-at-home / community service (HOSPICE_ATHOME_COD).
-- These are services, not residence, so they are resolved by HOSPICE_DISCHARGE_COD only.
-- Per-service flags are current when the latest service event is not superseded by a discharge.

WITH full_care AS (
    SELECT
        person_id,
        MAX(clinical_effective_date) AS latest_full_care_date
    FROM {{ ref('int_hospice_status_all') }}
    WHERE source_cluster_id = 'HOSPICE_FULLCARE_COD'
    GROUP BY person_id
),

at_home AS (
    SELECT
        person_id,
        MAX(clinical_effective_date) AS latest_at_home_date
    FROM {{ ref('int_hospice_status_all') }}
    WHERE source_cluster_id = 'HOSPICE_ATHOME_COD'
    GROUP BY person_id
),

discharge AS (
    SELECT
        person_id,
        MAX(clinical_effective_date) AS latest_discharge_date
    FROM {{ ref('int_hospice_status_all') }}
    WHERE source_cluster_id = 'HOSPICE_DISCHARGE_COD'
    GROUP BY person_id
),

latest_care AS (
    -- Latest care event overall (either service) for code/term details
    SELECT
        person_id,
        clinical_effective_date,
        concept_code,
        code_description,
        source_cluster_id
    FROM {{ ref('int_hospice_status_all') }}
    WHERE source_cluster_id IN ('HOSPICE_FULLCARE_COD', 'HOSPICE_ATHOME_COD')
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY person_id
        ORDER BY clinical_effective_date DESC, id DESC
    ) = 1
),

person_flags AS (
    SELECT
        COALESCE(fc.person_id, ah.person_id) AS person_id,
        fc.latest_full_care_date,
        ah.latest_at_home_date,
        d.latest_discharge_date,
        -- Service is current when its latest event is not superseded by a later discharge
        fc.latest_full_care_date IS NOT NULL
            AND fc.latest_full_care_date >= COALESCE(d.latest_discharge_date, '1900-01-01') AS is_hospice_full_care,
        ah.latest_at_home_date IS NOT NULL
            AND ah.latest_at_home_date >= COALESCE(d.latest_discharge_date, '1900-01-01') AS is_hospice_at_home
    FROM full_care fc
    FULL OUTER JOIN at_home ah
        ON fc.person_id = ah.person_id
    LEFT JOIN discharge d
        ON COALESCE(fc.person_id, ah.person_id) = d.person_id
)

SELECT
    pf.person_id,
    pp.sk_patient_id,
    lc.clinical_effective_date AS latest_hospice_care_date,
    pf.latest_full_care_date,
    pf.latest_at_home_date,
    pf.latest_discharge_date,
    lc.concept_code,
    lc.code_description,
    lc.source_cluster_id,
    pf.is_hospice_full_care,
    pf.is_hospice_at_home,
    pf.is_hospice_full_care OR pf.is_hospice_at_home AS is_under_hospice_care,
    CASE
        WHEN pf.is_hospice_full_care OR pf.is_hospice_at_home THEN 'Under Hospice Care'
        ELSE 'Discharged from Hospice'
    END AS hospice_care_status
FROM person_flags pf
JOIN {{ ref('int_patient_person_unique') }} pp
    ON pf.person_id = pp.person_id
LEFT JOIN latest_care lc
    ON pf.person_id = lc.person_id
QUALIFY ROW_NUMBER() OVER (
    PARTITION BY pf.person_id
    ORDER BY pp.sk_patient_id
) = 1
