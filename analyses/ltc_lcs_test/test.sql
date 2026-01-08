 SELECT
      'fct_person_palliative_care_register' as table_name,
      COUNT(*) as row_count,
      MAX(METADATA$ROW_CREATION_TIME) as last_updated
  FROM {{ ref('fct_person_palliative_care_register') }}

  Also, let me check if there's a subtle difference in the WHERE/aggregation logic. Can you run this to see what the production model's intermediate CTE calculates for one of those 12,829 people:

  -- Manually replicate production logic for one person
  WITH palliative_care_diagnoses AS (
      SELECT
          person_id,
          MIN(CASE WHEN is_palliative_care_code AND clinical_effective_date >= '2008-04-01'
              THEN clinical_effective_date END) AS earliest_diagnosis_date,
          MAX(CASE WHEN is_palliative_care_code AND clinical_effective_date >= '2008-04-01'
              THEN clinical_effective_date END) AS latest_diagnosis_date,
          MAX(CASE WHEN is_palliative_care_not_indicated_code
              THEN clinical_effective_date END) AS latest_no_longer_indicated_date
      FROM {{ ref('int_palliative_care_diagnoses_all') }}
      WHERE person_id = '00389AFD-F163-D7FA-E398-17EF5E225B55'  -- From your sample
      GROUP BY person_id
  )
  SELECT
      *,
      COALESCE(
          latest_diagnosis_date IS NOT NULL
          AND (latest_no_longer_indicated_date IS NULL OR latest_no_longer_indicated_date <= latest_diagnosis_date),
          FALSE
      ) AS calculated_is_on_register
  FROM palliative_care_diagnoses