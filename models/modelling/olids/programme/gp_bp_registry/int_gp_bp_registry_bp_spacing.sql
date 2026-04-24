{{
    config(
        materialized='table',
        cluster_by=['person_id'])
}}

/*
Per-person assessment of the BP spacing inclusion criterion:
"BP recorded on >= 4 separate dates >= 4 weeks apart over at least 36 consecutive months."

Method:
  - Greedy chain walk over each person's eligible BP readings (date-ordered):
    * Take the earliest reading as chain position 1.
    * Walk forward, including the next reading whose date is at least
      min_reading_gap_weeks (default 4) after the previously included reading.
    * Repeat until no further reading qualifies.
  - A person meets the criterion when the chain reaches
    min_qualifying_readings (default 4) and the span from first to last
    reading in the chain is at least min_span_months (default 36).

One row per person in the base population that has at least one eligible reading.
*/

WITH RECURSIVE eligible AS (

    SELECT
        person_id,
        effective_date,
        ROW_NUMBER() OVER (
            PARTITION BY person_id ORDER BY effective_date
        ) AS reading_rank
    FROM {{ ref('int_gp_bp_registry_bp_readings_eligible') }}

),

chain AS (

    -- Anchor: earliest reading per person is chain position 1
    SELECT
        person_id,
        effective_date,
        reading_rank,
        1 AS chain_position
    FROM eligible
    WHERE reading_rank = 1

    UNION ALL

    -- Recursive step: include the earliest subsequent reading
    -- that is at least min_reading_gap_weeks after the current chain reading
    SELECT
        r.person_id,
        r.effective_date,
        r.reading_rank,
        c.chain_position + 1
    FROM chain c
    INNER JOIN eligible r
        ON r.person_id = c.person_id
        AND r.reading_rank > c.reading_rank
        AND r.effective_date >= DATEADD(
            'week',
            {{ var('min_reading_gap_weeks', 4) }},
            c.effective_date
        )
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY c.person_id, c.chain_position ORDER BY r.effective_date
    ) = 1

),

summary AS (

    SELECT
        person_id,
        MAX(chain_position) AS qualifying_reading_count,
        MIN(effective_date) AS span_start,
        MAX(effective_date) AS span_end,
        DATEDIFF('day', MIN(effective_date), MAX(effective_date)) AS span_days
    FROM chain
    GROUP BY person_id

)

SELECT
    person_id,
    qualifying_reading_count,
    span_start,
    span_end,
    span_days,
    qualifying_reading_count >= {{ var('min_qualifying_readings', 4) }}
        AND span_days >= {{ var('min_span_months', 36) }} * 30
        AS meets_bp_criteria
FROM summary
