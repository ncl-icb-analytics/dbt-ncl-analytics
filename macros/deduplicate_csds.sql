-- 2025-10-13: dear future Tom, I removed an inner join that is necessary for tables which don't have unique_service_request_identifier
-- the inner join messed up the cyp201 deduplication process, so if you add it back in make sure it works ok
{% macro deduplicate_csds(
        csds_table,
        partition_cols = []
        ) %}

    SELECT
        ROW_NUMBER() OVER (
            PARTITION BY tbl.unique_service_request_identifier
            {%- for col in partition_cols %}
                , tbl.{{ col }}
            {%- endfor %}
            ORDER BY tbl.effective_from DESC
        ) AS sequence,
        tbl.* 

    FROM {{csds_table}} AS tbl

    INNER JOIN {{ref('raw_csds_activesubmission')}} AS a
        ON tbl.unique_submission_id = a.unique_submission_id
 
    QUALIFY sequence = 1

{% endmacro %}