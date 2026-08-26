-- Derives SUS OP POD, sensitive-category and legacy business-rule outputs.
{% macro sus_op_rule_code_matches(rule_name, attribute_name, value_expression, activity_date, include_or_exclude='INCLUDE') %}
    coalesce(
        array_contains(
            '{{ rule_name }}|{{ attribute_name }}|{{ include_or_exclude }}'::variant,
            matched.matched_rule_codes
        ),
        false
    )
{% endmacro %}

{% macro sus_op_pod_level_4(alias='') %}
    {{ sus_op_pod_level_4_from_values(alias ~ 'core_hrg', alias ~ 'main_specialty_code') }}
{% endmacro %}

{% macro sus_op_pod_level_4_from_values(op_hrg, main_spec) %}
    case
        when left(upper(trim({{ op_hrg }})), 2) not in ('WF', 'UZ') then 'OPPROC'
        else coalesce(
            (
                select min(mapping.pod_level_4)
                from {{ ref('sus_op_pod_mapping') }} as mapping
                where mapping.core_hrg = upper(trim({{ op_hrg }}))
                  and mapping.specialty_group in (
                      'ALL',
                      case
                          when trim({{ main_spec }}) = '560'
                            or trim({{ main_spec }}) between '900' and '960'
                              then 'NON_SPECIALIST'
                          else 'SPECIALIST'
                      end
                  )
            ),
            'Unknown'
        )
    end
{% endmacro %}

{% macro sus_op_sensitive_category(alias='') %}
    (
        select min_by(terminology.sensitive_category, terminology.priority)
        from {{ ref('sus_op_sensitive_terminology') }} as terminology
        where (terminology.code_system = 'ICD10'
               and terminology.code = upper(trim({{ alias }}primary_diagnosis_code)))
           or (terminology.code_system = 'OPCS4'
               and terminology.code = upper(trim({{ alias }}primary_procedure_code)))
    )
{% endmacro %}

{% macro sus_op_business_rule_string(alias='') %}
    nullif(concat(
        iff({{ alias }}rule_aecu_clinic_lnwht, '|AECU_CLINIC_LNWHT', ''),
        iff({{ alias }}rule_aecu_wa_lnwht, '|AECU_WA_LNWHT', ''),
        iff({{ alias }}rule_card_brent, '|CARD_BRENT', ''),
        iff({{ alias }}rule_card_icht, '|CARD_ICHT', ''),
        iff({{ alias }}rule_derm_cw, '|DERM_CW', ''),
        iff({{ alias }}rule_dup_icht, '|DUP_ICHT', ''),
        iff({{ alias }}rule_duplicate_lnwht, '|DUPLICATE_LNWHT', ''),
        iff({{ alias }}rule_ecg_cw, '|ECG_CW', ''),
        iff({{ alias }}rule_gum, '|GUM', ''),
        iff({{ alias }}rule_gyn_cw, '|GYN_CW', ''),
        iff({{ alias }}rule_in_health, '|IN_HEALTH', ''),
        iff({{ alias }}rule_mh_london_providers, '|MH_LondonProviders', ''),
        iff({{ alias }}rule_mh_psych, '|MH_Psych', ''),
        iff({{ alias }}rule_nc_nwl1, '|NC_NWL1', ''),
        iff({{ alias }}rule_nc_nwl2, '|NC_NWL2', ''),
        iff({{ alias }}rule_nc_nwl3, '|NC_NWL3', ''),
        iff({{ alias }}rule_nc_nwl4, '|NC_NWL4', ''),
        iff({{ alias }}rule_nc_nwl5, '|NC_NWL5', ''),
        iff({{ alias }}rule_nc_nwl6, '|NC_NWL6', ''),
        iff({{ alias }}rule_nc_nwl7, '|NC_NWL7', ''),
        iff({{ alias }}rule_opth_brent, '|OPTH_BRENT', ''),
        iff({{ alias }}rule_private, '|PRIVATE', ''),
        iff({{ alias }}rule_sleepclinic_wmuh, '|SLEEPCLINIC_WMUH', ''),
        iff({{ alias }}rule_sleepstudy_wmuh, '|SLEEPSTUDY_WMUH', ''),
        iff({{ alias }}rule_soaec_wmuh, '|SOAEC_WMUH', '')
    ), '')
{% endmacro %}

{% macro sus_op_contract_type(alias='') %}
    case
        /* Legacy procedure gives contract type 6 precedence over every other match. */
        when {{ alias }}rule_dup_icht or {{ alias }}rule_duplicate_lnwht then '6'
        /* Legacy contract type 7 is normalised to 2 after rule evaluation. */
        when {{ alias }}rule_private then '2'
        /* Preserve the legacy rule order while returning one usable category. */
        when {{ alias }}rule_aecu_clinic_lnwht or {{ alias }}rule_aecu_wa_lnwht then '1'
        when {{ alias }}rule_card_brent
          or {{ alias }}rule_card_icht
          or {{ alias }}rule_derm_cw
          or {{ alias }}rule_gyn_cw
          or {{ alias }}rule_opth_brent then '4'
        when {{ alias }}rule_ecg_cw or {{ alias }}rule_gum then iff({{ alias }}has_sla, '1', '2')
        when {{ alias }}rule_in_health then '5'
        when {{ alias }}rule_mh_london_providers
          or {{ alias }}rule_mh_psych
          or {{ alias }}rule_nc_nwl1
          or {{ alias }}rule_nc_nwl2
          or {{ alias }}rule_nc_nwl3
          or {{ alias }}rule_nc_nwl4
          or {{ alias }}rule_nc_nwl5
          or {{ alias }}rule_nc_nwl6
          or {{ alias }}rule_nc_nwl7 then iff({{ alias }}has_sla, '1', '2')
        when {{ alias }}rule_sleepclinic_wmuh
          or {{ alias }}rule_sleepstudy_wmuh
          or {{ alias }}rule_soaec_wmuh then '1'
        else iff({{ alias }}has_sla, '1', '2')
    end
{% endmacro %}
