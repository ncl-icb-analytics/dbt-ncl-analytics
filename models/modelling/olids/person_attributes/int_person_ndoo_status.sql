{{
    config(
        materialized='table',
        cluster_by=['person_id'])
}}

/*
Latest National Data Opt-Out preference per person.
Joins NDOO_HASHED -> int_patient_person_unique on sk_patient_id to resolve person_id.
One row per (person_id, preference_type) with is_opted_in derived from the latest record.
*/

WITH ndoo_resolved AS (
    SELECT
        pp.person_id,
        n.preference_type,
        n.preference_status,
        n.effective_from,
        n.effective_to,
        n.lds_start_date_time
    FROM {{ ref('stg_olids_ndoo_hashed') }} n
    INNER JOIN {{ ref('int_patient_person_unique') }} pp
        ON TRY_TO_NUMBER(n.sk_patient_id) = pp.sk_patient_id
    WHERE n.lds_is_deleted = FALSE
        AND n.is_latest = 1
)

SELECT
    person_id,
    preference_type,
    preference_status,
    preference_status = 'OPT_IN' AS is_opted_in,
    effective_from,
    effective_to
FROM ndoo_resolved
QUALIFY ROW_NUMBER() OVER (
    PARTITION BY person_id, preference_type
    ORDER BY COALESCE(effective_from, lds_start_date_time) DESC, lds_start_date_time DESC
) = 1
