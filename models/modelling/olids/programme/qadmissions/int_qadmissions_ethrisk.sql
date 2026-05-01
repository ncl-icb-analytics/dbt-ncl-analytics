{{
    config(
        materialized='table',
        cluster_by=['person_id'])
}}

/*
Ethnicity risk group (1..9) per person, for the QAdmissions feature
`ethrisk`. Numbering follows the registered QAdmissions model:

  1 = White / NotRecorded (default)   6 = Black Caribbean
  2 = Indian                          7 = Black African
  3 = Pakistani                       8 = Chinese
  4 = Bangladeshi                     9 = Other Ethnic Group
  5 = Other Asian

Pipeline
  dim_person_ethnicity_combi unions GP-coded ethnicity (via SNOMED) and
  event-level ethnicity (via BK ethnicity code) and produces one row per
  person with their latest non-blank `ethnicity` subcategory (e.g.
  "White: British", "Asian: Indian"). The model already filters out
  Recorded Not Known / Not Recorded / Not Stated / Refused at source.

  We join that subcategory text to the qadmissions_eth2016_to_ethrisk9
  seed to derive ethrisk. Any subcategory NOT in the seed defaults to 1
  (White / NotRecorded) - a defensive fallback; in practice every
  subcategory in dim_person_ethnicity_combi.ethnicity should be in the
  seed.

  Persons with no ethnicity record at all do not appear in
  dim_person_ethnicity_combi and so do not appear here; the consuming
  int_qadmissions_features model COALESCEs to 1 for those persons.

Grain: one row per person who has any usable ethnicity record.
*/

WITH ethnicity AS (
    SELECT
        person_id,
        ethnicity,
        code_date AS latest_ethnicity_date
    FROM {{ ref('dim_person_ethnicity_combi') }}
    WHERE person_id IS NOT NULL
)

SELECT
    e.person_id,
    e.ethnicity,
    e.latest_ethnicity_date,
    COALESCE(m.ethrisk, 1) AS ethrisk,
    COALESCE(m.qadmissions_label, 'White') AS qadmissions_label
FROM ethnicity e
LEFT JOIN {{ ref('qadmissions_eth2016_to_ethrisk9') }} m
    ON e.ethnicity = m.ethnicity
