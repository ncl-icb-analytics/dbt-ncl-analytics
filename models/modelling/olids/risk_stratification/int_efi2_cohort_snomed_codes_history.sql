{{
    config(
        materialized='table',
        cluster_by=['end_date', 'person_id'],
        tags=['efi2', 'monthly-full']
    )
}}

-- eFI2 observations valid at each scoring month. Deficits without a published
-- weight are removed before observations are expanded across month-ends.

WITH weighted_deficits AS (
    SELECT DISTINCT LOWER(deficit) AS deficit
    FROM {{ ref('stg_common_efi2_weights') }}
    WHERE weight IS NOT NULL
),

codelist AS (
    SELECT DISTINCT
        cd.snomedct_conceptid::VARCHAR AS snomed_code,
        cd.deficit,
        cd.other_instructions,
        cd.time_constraint_years,
        cd.age_limit
    FROM {{ ref('stg_common_efi2_codelists') }} AS cd
    INNER JOIN weighted_deficits AS wd
        ON LOWER(cd.deficit) = wd.deficit
),

relevant_observations AS (
    SELECT
        obs.id AS observation_id,
        obs.person_id,
        cd.deficit,
        cd.other_instructions,
        cd.time_constraint_years,
        cd.age_limit,
        obs.result_value,
        obs.clinical_effective_date
    FROM {{ ref('stg_olids_observation') }} AS obs
    INNER JOIN codelist AS cd
        ON obs.mapped_concept_code::VARCHAR = cd.snomed_code
)

SELECT
    obs.observation_id,
    pd.person_id,
    obs.deficit,
    obs.other_instructions,
    obs.result_value,
    obs.clinical_effective_date,
    DATEDIFF('year', pd.date_of_birth, DATE(obs.clinical_effective_date))
        AS age_at_event,
    pd.gender,
    pd.end_date
FROM relevant_observations AS obs
INNER JOIN {{ ref('int_efi2_patient_list_history') }} AS pd
    ON obs.person_id = pd.person_id
    AND obs.clinical_effective_date <= pd.end_date
    AND (
        DATEADD('year', obs.time_constraint_years, obs.clinical_effective_date)
            > pd.end_date
        OR obs.time_constraint_years IS NULL
    )
    AND (
        DATEDIFF('year', pd.date_of_birth, DATE(obs.clinical_effective_date))
            >= obs.age_limit
        OR obs.age_limit IS NULL
    )
