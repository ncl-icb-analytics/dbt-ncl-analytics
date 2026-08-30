{{
    config(
        materialized='table',
        cluster_by=['end_date', 'person_id'],
        tags=['monthly-full']
    )
}}

-- Shared monthly membership for all 17 segmentation LTCs.

SELECT
    person_id,
    end_date,
    condition_code,
    is_on_register
FROM {{ ref('int_segmentation_ltc_diagnosis_history') }}

UNION ALL

SELECT
    person_id,
    end_date,
    condition_code,
    is_on_register
FROM {{ ref('int_segmentation_ltc_rules_history') }}
