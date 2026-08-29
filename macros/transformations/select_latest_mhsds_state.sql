{% macro select_latest_mhsds_state(
        mhsds_table,
        partition_cols = [],
        tie_breaker_cols = []
    ) %}

    {# Select the newest reported state of a record that can recur across periods. #}
    {% if partition_cols | length == 0 %}
        {{ exceptions.raise_compiler_error(
            "You must provide at least one partition column to select_latest_mhsds_state."
        ) }}
    {% endif %}

    select tbl.*
    from {{ mhsds_table }} as tbl
    inner join {{ ref('raw_mhsds_activesubmission') }} as submission
        on tbl.uniq_submission_id = submission.uniq_submission_id
    qualify row_number() over (
        partition by
            {%- for col in partition_cols %}
                tbl.{{ col }}{% if not loop.last %}, {% endif %}
            {%- endfor %}
        order by
            submission.reporting_period_end_date desc
            , tbl.effective_from desc nulls last
            , tbl.uniq_submission_id desc
            {%- for col in tie_breaker_cols %}
            , tbl.{{ col }} desc
            {%- endfor %}
    ) = 1

{% endmacro %}
