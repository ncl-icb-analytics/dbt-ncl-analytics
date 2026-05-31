{{
    config(
        materialized='view',
        enabled=target.name in ['prod', 'snowflake-prod']
    )
}}

/*
Current dbt project health for semantic-layer freshness surfaces.

Elementary observability is only enabled in prod/snowflake-prod. In non-prod
targets this model returns no rows rather than failing to compile against
disabled package refs.
*/

WITH latest_invocation AS (
    SELECT *
    FROM {{ ref('dbt_invocations') }}
    WHERE target_name IN ('prod', 'snowflake-prod')
    QUALIFY ROW_NUMBER() OVER (ORDER BY generated_at DESC) = 1
),

invocation_run_results AS (
    SELECT *
    FROM {{ ref('dbt_run_results') }}
    WHERE invocation_id = (SELECT invocation_id FROM latest_invocation)
),

run_summary AS (
    SELECT
        COUNT_IF(resource_type = 'model') AS model_result_count,
        COUNT_IF(resource_type = 'model' AND status = 'success') AS model_success_count,
        COUNT_IF(resource_type = 'model' AND status != 'success') AS model_non_success_count,
        COUNT_IF(resource_type = 'test') AS test_result_count,
        COUNT_IF(resource_type = 'test' AND status IN ('fail', 'error')) AS failing_test_count,
        COUNT_IF(status = 'error') AS error_result_count,
        MAX(execute_completed_at) AS latest_result_completed_at
    FROM invocation_run_results
)

SELECT
    'dbt_project' AS pipeline_key,
    'dbt project health' AS display_name,
    inv.invocation_id,
    inv.command,
    inv.target_name,
    inv.full_refresh,
    inv.run_started_at,
    inv.run_completed_at,
    inv.generated_at,
    summary.latest_result_completed_at,
    summary.model_result_count,
    summary.model_success_count,
    summary.model_non_success_count,
    summary.test_result_count,
    summary.failing_test_count,
    summary.error_result_count,
    DATEDIFF('hour', COALESCE(inv.run_completed_at, inv.generated_at), CURRENT_TIMESTAMP()) AS lag_hours,
    CASE
        WHEN summary.error_result_count > 0 THEN 'error'
        WHEN summary.model_non_success_count > 0 THEN 'breach'
        WHEN summary.failing_test_count > 0 THEN 'stale'
        ELSE 'fresh'
    END AS pipeline_status
FROM latest_invocation inv
CROSS JOIN run_summary summary
