{% macro temporal_join(
        fact_table,
        fact_date_column,
        dimension_table,
        join_key='person_id',
        join_type='inner',
        valid_from_col='effective_start_date',
        valid_to_col='effective_end_date'
) %}
  {#-
      Point-in-time join: match each fact row to the dimension version whose validity
      window [valid_from, valid_to) contains the fact date.

      join_type    'inner' (default) drops fact rows with no valid version -- use for an
                   eligibility gate. 'left' keeps every fact row -- use for covariate
                   enrichment (absence is meaningful; coalesce defaults in the caller).
      valid_from_col / valid_to_col
                   SCD validity columns. Default to effective_start_date /
                   effective_end_date; pass dbt_valid_from / dbt_valid_to to join a dbt
                   snapshot. A NULL valid_to marks the current (open-ended) version.

      Emits a join fragment aliased `f` (fact) / `d` (dimension); use one call per
      dimension inside its own CTE.
  -#}
  {{- fact_table }} f
  {{ join_type | upper }} JOIN {{ dimension_table }} d
    ON f.{{ join_key }} = d.{{ join_key }}
    AND f.{{ fact_date_column }} >= d.{{ valid_from_col }}
    AND (d.{{ valid_to_col }} IS NULL OR f.{{ fact_date_column }} < d.{{ valid_to_col }})
{% endmacro %}