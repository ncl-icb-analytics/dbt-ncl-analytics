{# NHS financial period from a DATE. Financial year runs April-March; month 1 =
   April ... 12 = March. Used as the period-of-last-resort when a feed's stated
   financial year/month is absent (providers bill by activity date), mirroring
   the SLAM activity-date fallback (#806). #}

{# Financial month 1-12 from a date (April -> 1, March -> 12). #}
{% macro fin_month_from_date(d) %}
    mod(month({{ d }}) + 8, 12) + 1
{% endmacro %}

{# Financial year start as a bare 4-digit year (e.g. 2024-12-10 -> 2024,
   2025-02-10 -> 2024), matching the community feeds' YYYY convention. #}
{% macro fin_year_start_from_date(d) %}
    iff(month({{ d }}) >= 4, year({{ d }}), year({{ d }}) - 1)
{% endmacro %}
