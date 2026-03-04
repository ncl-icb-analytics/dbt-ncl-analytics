{% macro deduplicate_mhsds(
        mhsds_table,
        partition_cols = []
    ) %}

    {# 
        This macro deduplicates an mhsds table by keeping the most recent record
        (based on `effective_from`) within each unique combination of `partition_cols`.

        mhsds_table (dbt table ref):
            a ref to the raw mhsds table to be deduplicated
         
        partition_cols (list[str]):
            a list of column(s) present in the dedup_table which the deduplication occurs over

        Example usage:
          {{ 
                deduplicate_mhsds(mhsds_table = ref('raw_mhsds_mhs201carecontact'), 
                              partition_cols = ['uniq_serv_req_id', 'uniq_care_cont_id']) 
           }}
    #}

    {% if partition_cols | length == 0 %}
        {{ exceptions.raise_compiler_error("You must provide at least one partition column to deduplicate_mhsds.") }}
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
    FROM {{ mhsds_table }} AS tbl

    INNER JOIN {{ ref('raw_mhsds_activesubmission') }} AS a
        ON tbl.uniq_submission_id = a.uniq_submission_id

    QUALIFY sequence = 1

{% endmacro %}
