{% set code_sets = [
    ('clinical_response_priority', 'raw_ukhfd_data_dictionary_mhsds_clinical_response_priority'),
    ('primary_reason_for_referral', 'raw_ukhfd_data_dictionary_mhsds_reason_for_referral'),
    ('referring_care_professional_type', 'raw_ukhfd_data_dictionary_mhsds_referring_care_professional_type'),
    ('referring_care_professional_type', 'raw_ukhfd_data_dictionary_mhsds_referring_care_professional_staff_group_legacy'),
    ('referring_care_professional_staff_group', 'raw_ukhfd_data_dictionary_mhsds_referring_care_professional_staff_group'),
    ('referring_care_professional_staff_group', 'raw_ukhfd_data_dictionary_mhsds_referring_care_professional_staff_group_legacy'),
    ('rejection_reason', 'raw_ukhfd_data_dictionary_mhsds_referral_rejection_reason'),
    ('closure_reason', 'raw_ukhfd_data_dictionary_mhsds_referral_closure_reason'),
    ('out_of_area_referral_reason', 'raw_ukhfd_data_dictionary_mhsds_out_of_area_referral_reason')
] %}

{% for terminology_name, raw_model_name in code_sets %}
select
    '{{ terminology_name }}' as terminology_name,
    terminology.*
from (
    {{ select_ukhfd_data_dictionary_code_set(raw_model_name) }}
) as terminology
{% if not loop.last %}
union all
{% endif %}
{% endfor %}
