-- 2025-10-13: dear future Tom, I removed an inner join that is necessary for tables which don't have unique_service_request_identifier
-- the inner join messed up the cyp201 deduplication process, so if you add it back in make sure it works ok
{% macro deduplicate_csds(
        csds_table,
        partition_cols = []
    ) %}

    {# 
        This macro deduplicates a CSDS table by keeping the most recent record
        (based on `effective_from`) within each unique combination of `partition_cols`.

        Example usage:
          {{ deduplicate_csds('my_schema.my_table', ['unique_service_request_identifier', 'unique_care_contact_identifier']) }}
    #}

    {% if partition_cols | length == 0 %}
        {{ exceptions.raise_compiler_error("You must provide at least one partition column to deduplicate_csds.") }}
    {% endif %}

    SELECT
        ROW_NUMBER() OVER (
            PARTITION BY 
            {%- for col in partition_cols %}
                {{ "tbl." ~ col }}{% if not loop.last %}, {% endif %}
            {%- endfor %}
            ORDER BY tbl.effective_from DESC
        ) AS sequence,
        tbl.*
    FROM {{ csds_table }} AS tbl

    INNER JOIN {{ ref('raw_csds_activesubmission') }} AS a
        ON tbl.unique_submission_id = a.unique_submission_id

    QUALIFY sequence = 1

{% endmacro %}
