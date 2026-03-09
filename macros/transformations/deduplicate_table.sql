-- 2025-10-13: dear future Tom, I removed an inner join that is necessary for tables which don't have unique_service_request_identifier
-- the inner join messed up the cyp201 deduplication process, so if you add it back in make sure it works ok
{% macro deduplicate_table(
        table,
        partition_cols,
        order_cols
    ) %}

    {#
        This macro deduplicates a table by

        dedup_table (dbt table ref):
            a ref to the raw csds table to be deduplicated

        partition_cols (list[str]):
            a list of column(s) present in the dedup_table which the deduplication occurs over

        order_cols (list[str]):
            a list of column(s) to order by when picking the row to keep (DESC)

        Example usage:
    #}

    {% if partition_cols | length == 0 %}
        {{ exceptions.raise_compiler_error("You must provide at least one partition column to deduplicate_table.") }}
    {% endif %}

    {% if order_cols is not iterable or order_cols is string or order_cols | length == 0 %}
        {{ exceptions.raise_compiler_error("You must provide order_cols as a non-empty list to deduplicate_table.") }}
    {% endif %}

    SELECT
        tbl.*
    FROM {{ table }} AS tbl
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY
        {%- for col in partition_cols %}
            {{ "tbl." ~ col }}{% if not loop.last %}, {% endif %}
        {%- endfor %}
        ORDER BY
        {%- for col in order_cols %}
            tbl.{{ col }} DESC{% if not loop.last %},{% endif %}
        {%- endfor %}
    ) = 1

{% endmacro %}
