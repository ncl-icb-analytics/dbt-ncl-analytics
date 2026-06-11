{# Shared cleaning macros for the SDL SLAM contract-monitoring feeds
   (LSACM, LSPLCM, LSDrPLCM, LSDePLCM). Source values are provider-typed
   free text; these handle the formats observed in profiling (2026-06). #}

{# Money/activity string -> NUMBER(38,6).
   Handles thousands commas, currency symbols (incl. mojibake bytes), spaces,
   and accounting-style "(1,234.56)" negatives. 'TBC' and other non-numeric
   text -> NULL. #}
{% macro parse_slam_number(col) %}
    case
        when trim({{ col }}) rlike '^\\(.*\\)$'
            then -1 * try_to_number(regexp_replace({{ col }}, '[^0-9.]', ''), 38, 6)
        else try_to_number(regexp_replace({{ col }}, '[^0-9.-]', ''), 38, 6)
    end
{% endmacro %}

{# Provider-typed timestamp string -> TIMESTAMP_NTZ. Each branch is regex-gated
   so only well-formed inputs reach each parser. Observed formats:
     ISO (also with broken ' 00:00' suffixes), YYYY/MM/DD HH:MI,
     US MM/DD/YYYY AM/PM (SQL Server export), UK DD/MM/YYYY and DD-MM-YYYY
     with/without time, DD-Mon-YY(YY), Excel serial day numbers.
   Unparseable junk (month-only '2020-10 18:39', 'mm:ss.s' artefacts) -> NULL. #}
{% macro parse_slam_timestamp(col) %}
    case
        when {{ col }} rlike '^[0-9]{4}-[0-9]{2}-[0-9]{2}[ T][0-9]{1,2}:[0-9]{2}:[0-9]{2}.*'
            then coalesce(try_to_timestamp({{ col }}), try_to_timestamp(left({{ col }}, 19), 'YYYY-MM-DD HH24:MI:SS'))
        when {{ col }} rlike '^[0-9]{4}-[0-9]{2}-[0-9]{2}[ T][0-9]{1,2}:[0-9]{2}.*'
            then coalesce(try_to_timestamp({{ col }}), try_to_timestamp(left({{ col }}, 16), 'YYYY-MM-DD HH24:MI'))
        when {{ col }} rlike '^[0-9]{4}-[0-9]{2}-[0-9]{2}$'
            then try_to_timestamp({{ col }}, 'YYYY-MM-DD')
        when {{ col }} rlike '^[0-9]{4}/[0-9]{1,2}/[0-9]{1,2}[ ][0-9]{1,2}:[0-9]{2}$'
            then try_to_timestamp({{ col }}, 'YYYY/MM/DD HH24:MI')
        when upper({{ col }}) rlike '.*[ ](AM|PM)$'
            then try_to_timestamp({{ col }}, 'MM/DD/YYYY HH12:MI:SS AM')
        when {{ col }} rlike '^[0-9]{1,2}/[0-9]{1,2}/[0-9]{4}[ ][0-9]{1,2}:[0-9]{2}:[0-9]{2}.*'
            then try_to_timestamp({{ col }}, 'DD/MM/YYYY HH24:MI:SS')
        when {{ col }} rlike '^[0-9]{1,2}/[0-9]{1,2}/[0-9]{4}[ ][0-9]{1,2}:[0-9]{2}$'
            then try_to_timestamp({{ col }} || ':00', 'DD/MM/YYYY HH24:MI:SS')
        when {{ col }} rlike '^[0-9]{1,2}/[0-9]{1,2}/[0-9]{4}$'
            then try_to_timestamp({{ col }}, 'DD/MM/YYYY')
        when {{ col }} rlike '^[0-9]{1,2}-[0-9]{1,2}-[0-9]{4}[ ][0-9]{1,2}:[0-9]{2}:[0-9]{2}.*'
            then try_to_timestamp({{ col }}, 'DD-MM-YYYY HH24:MI:SS')
        when {{ col }} rlike '^[0-9]{1,2}-[0-9]{1,2}-[0-9]{4}$'
            then try_to_timestamp({{ col }}, 'DD-MM-YYYY')
        when {{ col }} rlike '^[0-9]{1,2}-[A-Za-z]{3}-[0-9]{4}.*'
            then try_to_timestamp({{ col }}, 'DD-MON-YYYY')
        when {{ col }} rlike '^[0-9]{1,2}-[A-Za-z]{3}-[0-9]{2}'
            then try_to_timestamp({{ col }}, 'DD-MON-YY')
        when {{ col }} rlike '^4[0-9]{4}(\\.[0-9]+)?$'
            then dateadd('second', round((try_to_number({{ col }}, 18, 6) - 25569) * 86400), '1970-01-01'::timestamp)
    end
{% endmacro %}

{# Financial year string -> canonical 'YYYYYY' (e.g. '202526'), validated so the
   second year follows the first. Accepts '202526', '2025/26', '2025-26',
   '2025 26', '2025-2026'. Bare calendar years ('2020') are ambiguous -> NULL.
   Garbage ('215551', specialty names on misaligned rows) -> NULL. #}
{% macro parse_slam_financial_year(col) %}
    case
        when regexp_replace({{ col }}, '[^0-9]', '') rlike '^20[0-9]{4}$'
             and try_to_number(substr(regexp_replace({{ col }}, '[^0-9]', ''), 5, 2))
                 = mod(try_to_number(substr(regexp_replace({{ col }}, '[^0-9]', ''), 3, 2)) + 1, 100)
            then regexp_replace({{ col }}, '[^0-9]', '')
        when regexp_replace({{ col }}, '[^0-9]', '') rlike '^20[0-9]{6}$'
             and try_to_number(substr(regexp_replace({{ col }}, '[^0-9]', ''), 5, 4))
                 = try_to_number(substr(regexp_replace({{ col }}, '[^0-9]', ''), 1, 4)) + 1
            then substr(regexp_replace({{ col }}, '[^0-9]', ''), 1, 4)
                 || substr(regexp_replace({{ col }}, '[^0-9]', ''), 7, 2)
    end
{% endmacro %}

{# Financial month -> INT 1-12, else NULL (junk like '110', '400' on
   misaligned rows). #}
{% macro parse_slam_financial_month(col) %}
    case
        when try_to_number(cast({{ col }} as varchar)) between 1 and 12
            then cast(try_to_number(cast({{ col }} as varchar)) as int)
    end
{% endmacro %}
