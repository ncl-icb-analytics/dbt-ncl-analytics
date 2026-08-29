{% macro select_active_mhsds_records(mhsds_table) %}

    {# Retain every record from the accepted submission for each reporting period. #}
    select tbl.*
    from {{ mhsds_table }} as tbl
    inner join {{ ref('raw_mhsds_activesubmission') }} as submission
        on tbl.uniq_submission_id = submission.uniq_submission_id

{% endmacro %}
