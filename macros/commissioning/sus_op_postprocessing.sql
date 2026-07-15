{% macro sus_op_pod_level_4(alias='') %}
    case
        when left(upper(trim({{ alias }}core_hrg)), 2) not in ('WF', 'UZ') then 'OPPROC'
        when upper(trim({{ alias }}core_hrg)) in ('WF01D', 'WF02D', 'WF01C', 'WF02C')
            then 'NON_FACE_TO_FACE'
        when upper(trim({{ alias }}core_hrg)) = 'WF01B'
             and not (trim({{ alias }}main_specialty_code) = '560'
                      or trim({{ alias }}main_specialty_code) between '900' and '960')
            then 'OPFASPCL'
        when upper(trim({{ alias }}core_hrg)) = 'WF02B'
             and not (trim({{ alias }}main_specialty_code) = '560'
                      or trim({{ alias }}main_specialty_code) between '900' and '960')
            then 'OPFAMPCL'
        when upper(trim({{ alias }}core_hrg)) = 'WF01A'
             and not (trim({{ alias }}main_specialty_code) = '560'
                      or trim({{ alias }}main_specialty_code) between '900' and '960')
            then 'OPFUPSPCL'
        when upper(trim({{ alias }}core_hrg)) = 'WF02A'
             and not (trim({{ alias }}main_specialty_code) = '560'
                      or trim({{ alias }}main_specialty_code) between '900' and '960')
            then 'OPFUPMPCL'
        when upper(trim({{ alias }}core_hrg)) = 'WF01B' then 'OPFASPNCL'
        when upper(trim({{ alias }}core_hrg)) = 'WF02B' then 'OPFAMPNCL'
        when upper(trim({{ alias }}core_hrg)) = 'WF01A' then 'OPFUPSPNCL'
        when upper(trim({{ alias }}core_hrg)) = 'WF02A' then 'OPFUPMPNCL'
        else 'Unknown'
    end
{% endmacro %}

{% macro sus_op_sensitive_category(alias='') %}
    case
        when {{ alias }}primary_diagnosis_code in (
            'B200','B201','B202','B203','B204','B205','B206','B207','B208','B209',
            'B210','B211','B212','B213','B217','B218','B219','B220','B221','B222',
            'B227','B230','B231','B232','B238','B24X','Z113','Z114','Z202','Z206',
            'Z21X','Z224','Z830'
        ) then 'HIV'
        when {{ alias }}primary_diagnosis_code in (
            'A500','A501','A502','A503','A504','A505','A506','A507','A509','A510',
            'A511','A512','A513','A514','A515','A519','A520','A521','A522','A523',
            'A527','A528','A529','A530','A539','A540','A541','A542','A543','A544',
            'A545','A546','A548','A549','A55X','A560','A561','A562','A563','A564',
            'A568','A630','A638','A634X'
        ) then 'STI'
        when {{ alias }}primary_diagnosis_code in ('Z311','Z312','Z313','Z318')
          or {{ alias }}primary_procedure_code in (
            'Q131','Q132','Q133','Q134','Q135','Q136','Q137','Q138','Q139','Q383',
            'Y961','Y962','Y963','Y964','Y965','Y966','Y968','Y969'
          ) then 'IVF'
    end
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
        when {{ alias }}rule_dup_icht or {{ alias }}rule_duplicate_lnwht then '|6'
        /* Legacy contract type 7 is normalised to 2 after rule evaluation. */
        when {{ alias }}rule_private then '|2'
        else coalesce(
            nullif(concat(
                iff({{ alias }}rule_aecu_clinic_lnwht, '|1', ''),
                iff({{ alias }}rule_aecu_wa_lnwht, '|1', ''),
                iff({{ alias }}rule_card_brent, '|4', ''),
                iff({{ alias }}rule_card_icht, '|4', ''),
                iff({{ alias }}rule_derm_cw, '|4', ''),
                iff({{ alias }}rule_ecg_cw, iff({{ alias }}has_sla, '|1', '|2'), ''),
                iff({{ alias }}rule_gum, iff({{ alias }}has_sla, '|1', '|2'), ''),
                iff({{ alias }}rule_gyn_cw, '|4', ''),
                iff({{ alias }}rule_in_health, '|5', ''),
                iff({{ alias }}rule_mh_london_providers, iff({{ alias }}has_sla, '|1', '|2'), ''),
                iff({{ alias }}rule_mh_psych, iff({{ alias }}has_sla, '|1', '|2'), ''),
                iff({{ alias }}rule_nc_nwl1, iff({{ alias }}has_sla, '|1', '|2'), ''),
                iff({{ alias }}rule_nc_nwl2, iff({{ alias }}has_sla, '|1', '|2'), ''),
                iff({{ alias }}rule_nc_nwl3, iff({{ alias }}has_sla, '|1', '|2'), ''),
                iff({{ alias }}rule_nc_nwl4, iff({{ alias }}has_sla, '|1', '|2'), ''),
                iff({{ alias }}rule_nc_nwl5, iff({{ alias }}has_sla, '|1', '|2'), ''),
                iff({{ alias }}rule_nc_nwl6, iff({{ alias }}has_sla, '|1', '|2'), ''),
                iff({{ alias }}rule_nc_nwl7, iff({{ alias }}has_sla, '|1', '|2'), ''),
                iff({{ alias }}rule_opth_brent, '|4', ''),
                iff({{ alias }}rule_sleepclinic_wmuh, '|1', ''),
                iff({{ alias }}rule_sleepstudy_wmuh, '|1', ''),
                iff({{ alias }}rule_soaec_wmuh, '|1', '')
            ), ''),
            iff({{ alias }}has_sla, '1', '2')
        )
    end
{% endmacro %}
