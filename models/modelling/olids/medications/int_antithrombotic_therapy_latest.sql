{{ config(materialized='table', cluster_by=['person_id']) }}

/*
Latest antiplatelet and oral anticoagulant orders per person, for "is being
taken" measures such as NICE IND132, IND133 and IND94. One row per person with
either class ever ordered. Antiplatelets are BNF 2.9; oral anticoagulants are
BNF 2.8.2. Orders dated before 1990 or after the build date are ignored.

Consumers apply their own window to the latest dates.
*/

WITH antiplatelet AS (
    SELECT
        person_id,
        MAX(order_date) AS latest_antiplatelet_order_date,
        COUNT(*) AS antiplatelet_order_count
    FROM {{ ref('int_antiplatelet_medications_all') }}
    WHERE order_date BETWEEN '1990-01-01' AND CURRENT_DATE()
    GROUP BY person_id
),

anticoagulant AS (
    SELECT
        person_id,
        MAX(order_date) AS latest_anticoagulant_order_date,
        COUNT(*) AS anticoagulant_order_count
    FROM {{ ref('int_anticoagulant_medications_all') }}
    WHERE order_date BETWEEN '1990-01-01' AND CURRENT_DATE()
    GROUP BY person_id
),

latest_anticoagulant_order AS (
    SELECT
        person_id,
        anticoagulant_type AS latest_anticoagulant_type
    FROM {{ ref('int_anticoagulant_medications_all') }}
    WHERE order_date BETWEEN '1990-01-01' AND CURRENT_DATE()
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY person_id ORDER BY order_date DESC, medication_order_id DESC
    ) = 1
)

SELECT
    COALESCE(ap.person_id, ac.person_id) AS person_id,
    ap.latest_antiplatelet_order_date,
    ap.antiplatelet_order_count,
    ac.latest_anticoagulant_order_date,
    ac.anticoagulant_order_count,
    lao.latest_anticoagulant_type
FROM antiplatelet ap
FULL OUTER JOIN anticoagulant ac ON ap.person_id = ac.person_id
LEFT JOIN latest_anticoagulant_order lao ON COALESCE(ap.person_id, ac.person_id) = lao.person_id
