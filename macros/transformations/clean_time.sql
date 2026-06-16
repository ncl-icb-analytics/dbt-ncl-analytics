{# Return a provider time string only when it is actually a clock time
   (HH:MM or HH:MM:SS), stripped to digits/colon (drops fractional seconds and
   stray text). Values that are really dates misfiled in a time column
   (e.g. '09/11/2017', '2017-11-09') have no leading HH:MM and return NULL -
   recover those into the date field by also parsing the time column there. #}
{% macro clean_time(t) %}
    case
        when trim({{ t }}) rlike '^[0-9]{1,2}:[0-9]{2}'
            then left(regexp_replace(trim({{ t }}), '[^0-9:]', ''), 8)
    end
{% endmacro %}
