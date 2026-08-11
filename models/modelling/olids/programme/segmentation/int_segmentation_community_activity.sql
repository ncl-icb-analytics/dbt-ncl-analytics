{{
    config(
        materialized='table',
        cluster_by=['sk_patient_id'])
}}

-- Community services activity block for segmentation. Grain: one row per
-- sk_patient_id with any attended CSDS care contact in the rolling 12
-- months ending on the latest contact date (lag-aware).
--
-- int_csds_encounters is already attended contacts only (one row per
-- contact). Some CSDS records carry no sk_patient_id, so contact counts
-- are a floor; sk_patient_id '1' is a shared junk key and is excluded.

WITH cc_max_date AS (
    SELECT MAX(start_date) AS max_date
    FROM {{ ref('int_csds_encounters') }}
    WHERE start_date <= CURRENT_DATE()
)

SELECT
    e.sk_patient_id,
    COUNT(*) AS community_contacts_12mo
FROM {{ ref('int_csds_encounters') }} AS e
CROSS JOIN cc_max_date AS m
WHERE
    e.start_date BETWEEN DATEADD(MONTH, -12, m.max_date) AND m.max_date
    AND e.sk_patient_id IS NOT NULL
    AND e.sk_patient_id != '1'
GROUP BY e.sk_patient_id
