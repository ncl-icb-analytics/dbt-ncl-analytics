{# Parse a string column that may use mixed date formats from spreadsheet exports.
   Returns DATE. Each branch is gated by a regex so only well-formed inputs reach
   each parser — avoids Snowflake's lenient auto-format treating '22-12-14' as
   year 0022, or DD-MON-YY parsing '24-Apr-24' as year 0024.

   Branch order:
     1. ISO YYYY-MM-DD (with optional time suffix)
     2. UK DD/MM/YYYY (with optional time suffix; takes precedence over US for ambiguous)
     3. UK DD-MM-YYYY
     4. DD-Mon-YYYY  (Excel 4-digit year)
     5. DD-Mon-YY    (Excel default; RR infers century: 00-49 → 20xx, 50-99 → 19xx)
     6. US MM/DD/YYYY HH:MI:SS AM/PM (SQL Server export — only matched via AM/PM tell)
   Excel time-only artefacts (e.g. '00:00.0') match no branch → NULL.
#}
{# NB: Snowflake RLIKE treats \\d literally — use POSIX [0-9] / [A-Za-z]. #}
{% macro parse_uk_date(col) %}
    case
        when {{ col }} rlike '^[0-9]{4}-[0-9]{2}-[0-9]{2}.*'
            then try_to_date(left({{ col }}, 10), 'YYYY-MM-DD')
        when {{ col }} rlike '^[0-9]{1,2}/[0-9]{1,2}/[0-9]{4}.*'
            then try_to_date(split_part({{ col }}, ' ', 1), 'DD/MM/YYYY')
        when {{ col }} rlike '^[0-9]{1,2}-[0-9]{1,2}-[0-9]{4}.*'
            then try_to_date({{ col }}, 'DD-MM-YYYY')
        when {{ col }} rlike '^[0-9]{1,2}-[A-Za-z]{3}-[0-9]{4}.*'
            then try_to_date({{ col }}, 'DD-MON-YYYY')
        when {{ col }} rlike '^[0-9]{1,2}-[A-Za-z]{3}-[0-9]{2}'
            then try_to_date({{ col }}, 'DD-MON-YY')
        when upper({{ col }}) rlike '.*[ ](AM|PM)'
            then try_to_date(split_part({{ col }}, ' ', 1), 'MM/DD/YYYY')
    end
{% endmacro %}

{% macro parse_uk_timestamp(col) %}
    case
        when {{ col }} rlike '^[0-9]{4}-[0-9]{2}-[0-9]{2}.*'
            then try_to_timestamp({{ col }})
        when {{ col }} rlike '^[0-9]{1,2}/[0-9]{1,2}/[0-9]{4} [0-9]{1,2}:[0-9]{2}.*'
            then try_to_timestamp({{ col }}, 'DD/MM/YYYY HH24:MI:SS')
        when {{ col }} rlike '^[0-9]{1,2}/[0-9]{1,2}/[0-9]{4}'
            then try_to_timestamp({{ col }}, 'DD/MM/YYYY')
        when {{ col }} rlike '^[0-9]{1,2}-[A-Za-z]{3}-[0-9]{4}.*'
            then try_to_timestamp({{ col }}, 'DD-MON-YYYY')
        when {{ col }} rlike '^[0-9]{1,2}-[A-Za-z]{3}-[0-9]{2}'
            then try_to_timestamp({{ col }}, 'DD-MON-YY')
        when upper({{ col }}) rlike '.*[ ](AM|PM)'
            then try_to_timestamp({{ col }}, 'MM/DD/YYYY HH12:MI:SS AM')
    end
{% endmacro %}

{% macro parse_uk_timestamp(col) %}
    coalesce(
        try_to_timestamp({{ col }}),
        try_to_timestamp({{ col }}, 'DD/MM/YYYY HH24:MI:SS'),
        try_to_timestamp({{ col }}, 'DD/MM/YYYY HH:MI:SS'),
        try_to_timestamp({{ col }}, 'DD/MM/YYYY')
    )
{% endmacro %}
