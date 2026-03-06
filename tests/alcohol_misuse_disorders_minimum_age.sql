SELECT *
FROM {{ ref('int_alcohol_misuse_disorders') }} AS disorder
WHERE clinical_effective_date IS NOT NULL
  AND person_id IS NOT NULL
  AND EXISTS (
      SELECT 1
      FROM ({{ get_observations("'ALCOHOL_MISUSE_DISORDERS'") }}) obs
      WHERE obs.id = disorder.id
        AND obs.cluster_id = disorder.source_cluster_id
        AND obs.age_at_event < 16
  )
