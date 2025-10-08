{% macro generate_table_comment() %}
  {%- if execute -%}
    {%- set model_description = model.description or "" -%}
    {%- set current_user = target.user | default('SYSTEM') -%}

    {#- Convert UTC to Europe/London timezone -#}
    {%- set pytz = modules.pytz -%}
    {%- set london_tz = pytz.timezone('Europe/London') -%}
    {%- set london_time = run_started_at.astimezone(london_tz) -%}

    {%- set day = london_time.strftime('%d')|int -%}
    {%- set day_suffix = 'th' -%}
    {%- if day % 10 == 1 and day != 11 -%}{%- set day_suffix = 'st' -%}{%- endif -%}
    {%- if day % 10 == 2 and day != 12 -%}{%- set day_suffix = 'nd' -%}{%- endif -%}
    {%- if day % 10 == 3 and day != 13 -%}{%- set day_suffix = 'rd' -%}{%- endif -%}
    {%- set tz_abbr = london_time.strftime('%Z') -%}
    {%- set run_timestamp = day|string + day_suffix + london_time.strftime(' %B %Y at %H:%M:%S ') + tz_abbr -%}
    {%- set target_name = target.name | default('unknown') -%}

    {%- set github_base_url = "https://github.com/ncl-icb-analytics/dbt-ncl-analytics/blob/main/" -%}
    {%- set model_file_path = model.original_file_path | replace("\\", "/") -%}
    {%- set github_file_url = github_base_url + model_file_path -%}

    {#- Check for custom message in meta config -#}
    {%- set model_meta = config.get('meta', {}) -%}
    {%- set custom_message = model_meta.get('custom_message', '') -%}
    {%- if custom_message -%}
      {%- set custom_message = "⚠️ " + custom_message + "\n\n" -%}
    {%- endif -%}

    {%- if model_description or custom_message -%}
      {%- set clean_description = (custom_message + model_description) | replace("'", "''") -%}
      {%- set footer = "

🤖 Last ran on " + run_timestamp + " by " + current_user + " (target: " + target_name + ")
📄 Model source: " + github_file_url + "
📖 Documentation: https://github.com/ncl-icb-analytics/dbt-ncl-analytics" -%}
      {{- clean_description + footer | replace("'", "''") -}}
    {%- else -%}
🤖 Last ran on {{ run_timestamp }} by {{ current_user }} (target: {{ target_name }})
📄 Model source: {{ github_file_url }}
📖 Documentation: https://github.com/ncl-icb-analytics/dbt-ncl-analytics
    {%- endif -%}
  {%- endif -%}
{% endmacro %}