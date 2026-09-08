{% set code_sets = [
    ('priority_type', 'raw_ukhfd_data_dictionary_priority_type'),
    ('reason_for_referral', 'raw_ukhfd_data_dictionary_csds_reason_for_referral'),
    ('referring_staff_group', 'raw_ukhfd_data_dictionary_csds_referring_staff_group')
] %}

{% for code_set_name, raw_model_name in code_sets %}
select '{{ code_set_name }}' as code_set_name, definitions.*
from ({{ select_ukhfd_data_dictionary_code_set(raw_model_name) }}) as definitions
{% if not loop.last %}union all{% endif %}
{% endfor %}
