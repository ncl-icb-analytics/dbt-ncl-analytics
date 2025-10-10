{{
    config(
        materialized='table',
        cluster_by=['person_id']
    )
}}

/*
Latest Rockwood Clinical Frailty Scale score per person.
Uses most recent assessment date, with ID as tiebreaker.
*/

SELECT
    person_id,
    ID,
    clinical_effective_date,
    concept_code,
    concept_display,
    source_cluster_id,
    rockwood_score,
    rockwood_description,
    frailty_level,
    frailty_category,
    is_valid_rockwood_code,
    is_frail,
    is_severely_frail
FROM {{ ref('int_rockwood_all') }}
QUALIFY ROW_NUMBER() OVER (
    PARTITION BY person_id
    ORDER BY clinical_effective_date DESC, ID DESC
) = 1