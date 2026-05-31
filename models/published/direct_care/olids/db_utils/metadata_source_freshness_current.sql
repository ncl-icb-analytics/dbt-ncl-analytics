{{
    config(
        materialized='view'
    )
}}

/*
Current-state source freshness for the semantic layer.

This first pass intentionally focuses on the logical sources we currently care
about in the semantic layer:
  - OLIDS, split into core operational subdomains
  - PDS
  - Deaths Registry

For the OLIDS subdomains we prefer consensus business/event dates over
warehouse table metadata. That mirrors the existing global refresh-date
approach: future-dated records are excluded, stale/non-flowing practices are
filtered out by requiring consensus, and the resulting signal better reflects
true source-data currency than LAST_ALTERED alone.
*/

WITH source_config AS (
    SELECT
        source_key,
        display_name,
        NULLIF(parent_source_key, '') AS parent_source_key,
        source_group,
        sort_order,
        include_in_rollup,
        is_reference_data,
        warn_after_hours,
        breach_after_hours,
        basis_models
    FROM {{ ref('source_freshness_config') }}
),

olids_observation_dates AS (
    SELECT
        clinical_effective_date::date AS effective_date,
        publisher_organisation_code
    FROM {{ ref('stg_olids_observation') }}
    WHERE clinical_effective_date < CURRENT_DATE()
      AND clinical_effective_date >= DATEADD('month', -3, CURRENT_DATE())
      AND publisher_organisation_code IS NOT NULL
),
olids_observation_consensus AS (
    SELECT
        'olids_observations' AS source_key,
        effective_date AS freshness_date,
        effective_date::timestamp_ntz AS freshness_ts,
        practice_count AS contributing_organisation_count,
        150 AS consensus_threshold,
        'consensus_business_date' AS freshness_basis,
        'Latest clinical_effective_date with at least 150 publisher organisations contributing observations.' AS basis_detail
    FROM (
        SELECT
            effective_date,
            COUNT(DISTINCT publisher_organisation_code) AS practice_count
        FROM olids_observation_dates
        GROUP BY effective_date
    )
    WHERE practice_count >= 150
    QUALIFY ROW_NUMBER() OVER (ORDER BY effective_date DESC) = 1
),

olids_appointment_dates AS (
    SELECT
        start_date::date AS effective_date,
        publisher_organisation_code
    FROM {{ ref('stg_olids_appointment') }}
    WHERE start_date < CURRENT_DATE()
      AND start_date >= DATEADD('month', -3, CURRENT_DATE())
      AND publisher_organisation_code IS NOT NULL
),
olids_appointment_consensus AS (
    SELECT
        'olids_appointments' AS source_key,
        effective_date AS freshness_date,
        effective_date::timestamp_ntz AS freshness_ts,
        practice_count AS contributing_organisation_count,
        150 AS consensus_threshold,
        'consensus_business_date' AS freshness_basis,
        'Latest appointment start_date with at least 150 publisher organisations contributing non-future appointments.' AS basis_detail
    FROM (
        SELECT
            effective_date,
            COUNT(DISTINCT publisher_organisation_code) AS practice_count
        FROM olids_appointment_dates
        GROUP BY effective_date
    )
    WHERE practice_count >= 150
    QUALIFY ROW_NUMBER() OVER (ORDER BY effective_date DESC) = 1
),

olids_medication_dates AS (
    SELECT
        COALESCE(clinical_effective_date::date, date_recorded::date) AS effective_date,
        publisher_organisation_code
    FROM {{ ref('stg_olids_medication_order') }}
    WHERE COALESCE(clinical_effective_date::date, date_recorded::date) < CURRENT_DATE()
      AND COALESCE(clinical_effective_date::date, date_recorded::date) >= DATEADD('month', -3, CURRENT_DATE())
      AND publisher_organisation_code IS NOT NULL

    UNION ALL

    SELECT
        COALESCE(clinical_effective_date::date, date_recorded::date) AS effective_date,
        publisher_organisation_code
    FROM {{ ref('stg_olids_medication_statement') }}
    WHERE COALESCE(clinical_effective_date::date, date_recorded::date) < CURRENT_DATE()
      AND COALESCE(clinical_effective_date::date, date_recorded::date) >= DATEADD('month', -3, CURRENT_DATE())
      AND publisher_organisation_code IS NOT NULL
),
olids_medication_consensus AS (
    SELECT
        'olids_medications' AS source_key,
        effective_date AS freshness_date,
        effective_date::timestamp_ntz AS freshness_ts,
        practice_count AS contributing_organisation_count,
        150 AS consensus_threshold,
        'consensus_business_date' AS freshness_basis,
        'Latest medication clinical_effective_date/date_recorded with at least 150 publisher organisations contributing medication data.' AS basis_detail
    FROM (
        SELECT
            effective_date,
            COUNT(DISTINCT publisher_organisation_code) AS practice_count
        FROM olids_medication_dates
        GROUP BY effective_date
    )
    WHERE practice_count >= 150
    QUALIFY ROW_NUMBER() OVER (ORDER BY effective_date DESC) = 1
),

olids_registration_dates AS (
    SELECT
        COALESCE(
            lds_datetime_update_acquired::date,
            episode_of_care_start_date::date,
            episode_of_care_end_date::date
        ) AS effective_date,
        publisher_organisation_code
    FROM {{ ref('stg_olids_episode_of_care') }}
    WHERE COALESCE(
            lds_datetime_update_acquired::date,
            episode_of_care_start_date::date,
            episode_of_care_end_date::date
        ) < CURRENT_DATE()
      AND COALESCE(
            lds_datetime_update_acquired::date,
            episode_of_care_start_date::date,
            episode_of_care_end_date::date
        ) >= DATEADD('month', -3, CURRENT_DATE())
      AND publisher_organisation_code IS NOT NULL
),
olids_episode_of_care_consensus AS (
    SELECT
        'olids_episode_of_care' AS source_key,
        effective_date AS freshness_date,
        effective_date::timestamp_ntz AS freshness_ts,
        practice_count AS contributing_organisation_count,
        150 AS consensus_threshold,
        'consensus_update_date' AS freshness_basis,
        'Latest episode-of-care update date with at least 150 publisher organisations contributing episode-of-care records.' AS basis_detail
    FROM (
        SELECT
            effective_date,
            COUNT(DISTINCT publisher_organisation_code) AS practice_count
        FROM olids_registration_dates
        GROUP BY effective_date
    )
    WHERE practice_count >= 150
    QUALIFY ROW_NUMBER() OVER (ORDER BY effective_date DESC) = 1
),

non_olids_metadata AS (
    SELECT
        'pds' AS source_key,
        last_altered::date AS freshness_date,
        last_altered AS freshness_ts,
        NULL::integer AS contributing_organisation_count,
        NULL::integer AS consensus_threshold,
        'snowflake_last_altered' AS freshness_basis,
        'Last altered timestamp for DATA_LAKE.PDS.PDS_PERSON.' AS basis_detail
    FROM DATA_LAKE.INFORMATION_SCHEMA.TABLES
    WHERE table_schema = 'PDS'
      AND UPPER(table_name) = 'PDS_PERSON'

    UNION ALL

    SELECT
        'registries_deaths' AS source_key,
        last_altered::date AS freshness_date,
        last_altered AS freshness_ts,
        NULL::integer AS contributing_organisation_count,
        NULL::integer AS consensus_threshold,
        'snowflake_last_altered' AS freshness_basis,
        'Last altered timestamp for DATA_LAKE.DEATHS.DEATHS.' AS basis_detail
    FROM DATA_LAKE.INFORMATION_SCHEMA.TABLES
    WHERE table_schema = 'DEATHS'
      AND UPPER(table_name) = 'DEATHS'
),

atomic_current AS (
    SELECT * FROM olids_observation_consensus
    UNION ALL
    SELECT * FROM olids_appointment_consensus
    UNION ALL
    SELECT * FROM olids_medication_consensus
    UNION ALL
    SELECT * FROM olids_episode_of_care_consensus
    UNION ALL
    SELECT * FROM non_olids_metadata
),

rollup_current AS (
    SELECT
        'olids' AS source_key,
        MIN(freshness_date) AS freshness_date,
        MIN(freshness_ts) AS freshness_ts,
        NULL::integer AS contributing_organisation_count,
        NULL::integer AS consensus_threshold,
        'rollup_floor' AS freshness_basis,
        'Floor of OLIDS episode-of-care, appointments, observations, and medications freshness.' AS basis_detail
    FROM atomic_current
    WHERE source_key IN (
        'olids_episode_of_care',
        'olids_appointments',
        'olids_observations',
        'olids_medications'
    )
),

combined_current AS (
    SELECT * FROM atomic_current
    UNION ALL
    SELECT * FROM rollup_current
),

enriched AS (
    SELECT
        cfg.source_key,
        cfg.display_name,
        cfg.parent_source_key,
        cfg.source_group,
        cfg.sort_order,
        cfg.include_in_rollup,
        cfg.is_reference_data,
        cfg.warn_after_hours,
        cfg.breach_after_hours,
        cfg.basis_models,
        cur.freshness_date,
        cur.freshness_ts,
        cur.contributing_organisation_count,
        cur.consensus_threshold,
        cur.freshness_basis,
        cur.basis_detail,
        CURRENT_TIMESTAMP() AS measured_at,
        DATEDIFF('hour', cur.freshness_ts, CURRENT_TIMESTAMP()) AS lag_hours,
        DATEDIFF('day', cur.freshness_date, CURRENT_DATE()) AS lag_days
    FROM source_config cfg
    LEFT JOIN combined_current cur
      ON cfg.source_key = cur.source_key
)

SELECT
    source_key,
    display_name,
    parent_source_key,
    source_group,
    sort_order,
    include_in_rollup,
    is_reference_data,
    warn_after_hours,
    breach_after_hours,
    basis_models,
    freshness_date,
    freshness_ts,
    contributing_organisation_count,
    consensus_threshold,
    freshness_basis,
    basis_detail,
    measured_at,
    lag_hours,
    lag_days,
    CASE
        WHEN freshness_ts IS NULL THEN 'error'
        WHEN lag_hours <= warn_after_hours THEN 'fresh'
        WHEN lag_hours <= breach_after_hours THEN 'stale'
        ELSE 'breach'
    END AS freshness_status
FROM enriched
ORDER BY sort_order, source_key
