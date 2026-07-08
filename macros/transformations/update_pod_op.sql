-- macros/update_pod_op.sql
{% macro update_pod_op(op_hrg, main_spec) %}
    {%- set hrg = "upper(trim(" ~ op_hrg ~ "))" -%}
    {%- set spec = "trim(" ~ main_spec ~ ")" -%}
    case
        when left({{ hrg }}, 2) <> 'WF' and left({{ hrg }}, 2) <> 'UZ' then 'OPPROC'
        when {{ hrg }} in ('WF01D', 'WF02D', 'WF01C', 'WF02C') then 'NON_FACE_TO_FACE'
        when left({{ hrg }}, 2) = 'WF'
             and not ({{ spec }} = '560' or ({{ spec }} between '900' and '960')) then
            case
                when {{ hrg }} = 'WF01B' then 'OPFASPCL'
                when {{ hrg }} = 'WF02B' then 'OPFAMPCL'
                when {{ hrg }} = 'WF01A' then 'OPFUPSPCL'
                when {{ hrg }} = 'WF02A' then 'OPFUPMPCL'
            end
        when left({{ hrg }}, 2) = 'WF'
             and ({{ spec }} = '560' or ({{ spec }} between '900' and '960')) then
            case
                when {{ hrg }} = 'WF01B' then 'OPFASPNCL'
                when {{ hrg }} = 'WF02B' then 'OPFAMPNCL'
                when {{ hrg }} = 'WF01A' then 'OPFUPSPNCL'
                when {{ hrg }} = 'WF02A' then 'OPFUPMPNCL'
            end
        else 'Unknown'
    end
{% endmacro %}