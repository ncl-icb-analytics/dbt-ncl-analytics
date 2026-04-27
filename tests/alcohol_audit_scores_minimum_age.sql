SELECT *
FROM {{ ref('int_alcohol_audit_scores') }} AS audit
WHERE clinical_effective_date IS NOT NULL
  AND person_id IS NOT NULL
  AND EXISTS (
      SELECT 1
      FROM ({{ get_observations("'AUDITC_COD', 'AUDIT_COD'") }}) obs
      WHERE obs.id = audit.id
        AND obs.cluster_id = audit.source_cluster_id
        AND obs.age_at_event < 16
  )
