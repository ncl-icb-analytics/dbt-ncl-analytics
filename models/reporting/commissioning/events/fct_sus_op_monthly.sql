{{
    config(
        materialized='table',
        tags=['monthly', 'sus', 'outpatient']
    )
}}

/*
    Declarative replacement for ETL.postProcess_OP_{Current,Rec,PostRec}
    and dbo.processBusinessRules for the single upstream Date Range feed.
*/

with base as (
    select *
    from {{ ref('int_sus_op_monthly') }}
),

dedupe_ranked as (
    select
        b.*,
        case
            when b.attended_or_did_not_attend in ('5', '6')
             and b.sk_patient_id <> 1
                then row_number() over (
                    partition by
                        coalesce(to_varchar(b.sk_patient_id), b.local_patient_identifier, '0'),
                        coalesce(left(b.organisation_code_code_of_provider, 3), '0'),
                        b.appointment_date,
                        coalesce(to_varchar(b.first_attendance), '0'),
                        coalesce(b.treatment_function_code, '0'),
                        coalesce(to_varchar(b.attended_or_did_not_attend), '0')
                    order by b.sk_encounter_id
                )
        end as duplicate_rank
    from base as b
),

reference_enriched as (
    select
        d.*,
        case
            when diagnosis_term.priority is null then procedure_term.sensitive_category
            when procedure_term.priority is null
              or diagnosis_term.priority <= procedure_term.priority
                then diagnosis_term.sensitive_category
            else procedure_term.sensitive_category
        end as mapped_sensitive_category,
        case
            when left(upper(trim(d.core_hrg)), 2) not in ('WF', 'UZ') then 'OPPROC'
            else coalesce(pod_mapping.pod_level_4, 'Unknown')
        end as mapped_pod_level_4
    from dedupe_ranked as d
    left join {{ ref('sus_op_pod_mapping') }} as pod_mapping
        on pod_mapping.core_hrg = upper(trim(d.core_hrg))
       and pod_mapping.specialty_group in (
            'ALL',
            case
                when trim(d.main_specialty_code) = '560'
                  or trim(d.main_specialty_code) between '900' and '960'
                    then 'NON_SPECIALIST'
                else 'SPECIALIST'
            end
       )
    left join {{ ref('sus_op_sensitive_terminology') }} as diagnosis_term
        on diagnosis_term.code_system = 'ICD10'
       and diagnosis_term.code = upper(trim(d.primary_diagnosis_code))
    left join {{ ref('sus_op_sensitive_terminology') }} as procedure_term
        on procedure_term.code_system = 'OPCS4'
       and procedure_term.code = upper(trim(d.primary_procedure_code))
),

postprocessed as (
    select
        d.* exclude (duplicate_rank, mapped_sensitive_category, mapped_pod_level_4)
        replace (
            iff(d.duplicate_rank > 1, 2, d.zcommissioning_access) as zcommissioning_access,
            coalesce(d.zsensitive_data_category, d.mapped_sensitive_category)
                as zsensitive_data_category,
            coalesce(d.zpod_level_4, d.mapped_pod_level_4) as zpod_level_4,
            iff(d.tariff_total_payment_national is not null,
                d.tariff_total_payment_national, d.zderivedprice) as zderivedprice,
            iff(d.tariff_total_payment_national is not null, 'PBR', d.zderivedpriceflag)
                as zderivedpriceflag,
            iff(d.tariff_total_payment_national is not null, current_timestamp(), d.zderivedpricedt)
                as zderivedpricedt,
            d.local_patient_identifier as zlocalpatientidentifier,
            coalesce(d.zcarehome, ch.care_home_code) as zcarehome
        )
    from reference_enriched as d
    left join {{ ref('int_sus_op_care_home') }} as ch
        on d.sk_encounter_id = ch.sk_encounter_id
),

rule_code_matches as (
    select
        p.sk_encounter_id,
        array_agg(distinct
            rule_code.rule_name || '|' || rule_code.attribute_name || '|'
            || rule_code.include_or_exclude
        ) as matched_rule_codes
    from postprocessed as p
    inner join {{ ref('sus_op_business_rule_codes') }} as rule_code
        on (rule_code.effective_from is null or p.appointment_date >= rule_code.effective_from)
       and (rule_code.effective_to is null or p.appointment_date <= rule_code.effective_to)
       and case rule_code.match_type
            when 'EXACT' then
                upper(trim(
                    case rule_code.attribute_name
                        when 'provider_code' then left(p.organisation_code_code_of_provider, 3)
                        when 'site_code' then p.site_code_of_treatment
                        when 'provider_site_code' then p.provider_site_code
                        when 'consultant_code' then p.consultant_code
                        when 'commissioner_code' then p.organisation_code_code_of_commissioner
                        when 'treatment_function_code' then p.treatment_function_code
                        when 'provider_reference_no' then p.provider_reference_no
                    end
                )) = upper(rule_code.code)
            when 'PREFIX' then
                upper(case rule_code.attribute_name
                    when 'provider_reference_no' then p.provider_reference_no
                end) like upper(rule_code.code) || '%'
            when 'CONTAINS' then
                upper(case rule_code.attribute_name
                    when 'provider_reference_no' then p.provider_reference_no
                end) like '%' || upper(rule_code.code) || '%'
           end
    group by p.sk_encounter_id
),

rule_flags as (
    select
        p.*,
        {{ sus_op_rule_code_matches('AECU_CLINIC_LNWHT', 'provider_code', "left(p.organisation_code_code_of_provider, 3)", 'p.appointment_date') }}
            and {{ sus_op_rule_code_matches('AECU_CLINIC_LNWHT', 'provider_reference_no', 'p.provider_reference_no', 'p.appointment_date') }} as rule_aecu_clinic_lnwht,
        {{ sus_op_rule_code_matches('AECU_WA_LNWHT', 'provider_code', "left(p.organisation_code_code_of_provider, 3)", 'p.appointment_date') }}
            and {{ sus_op_rule_code_matches('AECU_WA_LNWHT', 'provider_reference_no', 'p.provider_reference_no', 'p.appointment_date') }} as rule_aecu_wa_lnwht,
        {{ sus_op_rule_code_matches('CARD_BRENT', 'provider_code', "left(p.organisation_code_code_of_provider, 3)", 'p.appointment_date') }}
            and {{ sus_op_rule_code_matches('CARD_BRENT', 'consultant_code', 'p.consultant_code', 'p.appointment_date') }}
            and {{ sus_op_rule_code_matches('CARD_BRENT', 'treatment_function_code', 'p.treatment_function_code', 'p.appointment_date') }}
            and {{ sus_op_rule_code_matches('CARD_BRENT', 'commissioner_code', 'p.organisation_code_code_of_commissioner', 'p.appointment_date') }} as rule_card_brent,
        (
            {{ sus_op_rule_code_matches('CARD_ICHT_ANY', 'provider_reference_no', 'p.provider_reference_no', 'p.appointment_date') }}
            or (
                {{ sus_op_rule_code_matches('CARD_ICHT', 'provider_reference_no', 'p.provider_reference_no', 'p.appointment_date') }}
                and {{ sus_op_rule_code_matches('CARD_ICHT', 'provider_code', "left(p.organisation_code_code_of_provider, 3)", 'p.appointment_date') }}
            )
        ) as rule_card_icht,
        {{ sus_op_rule_code_matches('DERM_CW', 'site_code', 'p.site_code_of_treatment', 'p.appointment_date') }} as rule_derm_cw,
        {{ sus_op_rule_code_matches('DUP_ICHT', 'provider_reference_no', 'p.provider_reference_no', 'p.appointment_date') }}
            and {{ sus_op_rule_code_matches('DUP_ICHT', 'provider_code', "left(p.organisation_code_code_of_provider, 3)", 'p.appointment_date') }} as rule_dup_icht,
        {{ sus_op_rule_code_matches('DUPLICATE_LNWHT', 'provider_code', "left(p.organisation_code_code_of_provider, 3)", 'p.appointment_date') }} as rule_duplicate_lnwht,
        {{ sus_op_rule_code_matches('ECG_CW', 'treatment_function_code', 'p.treatment_function_code', 'p.appointment_date') }}
            and {{ sus_op_rule_code_matches('ECG_CW', 'provider_code', "left(p.organisation_code_code_of_provider, 3)", 'p.appointment_date') }} as rule_ecg_cw,
        {{ sus_op_rule_code_matches('GUM', 'treatment_function_code', 'p.treatment_function_code', 'p.appointment_date') }} as rule_gum,
        {{ sus_op_rule_code_matches('GYN_CW', 'site_code', 'p.site_code_of_treatment', 'p.appointment_date') }} as rule_gyn_cw,
        {{ sus_op_rule_code_matches('IN_HEALTH', 'provider_code', "left(p.organisation_code_code_of_provider, 3)", 'p.appointment_date') }} as rule_in_health,
        {{ sus_op_rule_code_matches('MH_LondonProviders', 'provider_code', "left(p.organisation_code_code_of_provider, 3)", 'p.appointment_date') }} as rule_mh_london_providers,
        left(p.treatment_function_code, 1) = '7' as rule_mh_psych,
        (
            ({{ sus_op_rule_code_matches('NC_NWL', 'provider_code', "left(p.organisation_code_code_of_provider, 3)", 'p.appointment_date') }}
             and p.commissioning_serial_no_agreement_no = 'NCB')
            or ({{ sus_op_rule_code_matches('NC_NWL', 'site_code', 'p.site_code_of_treatment', 'p.appointment_date') }}
                and p.commissioning_serial_no_agreement_no = 'NCB')
        ) as rule_nc_nwl1,
        (
            ({{ sus_op_rule_code_matches('NC_NWL', 'provider_code', "left(p.organisation_code_code_of_provider, 3)", 'p.appointment_date') }}
             and p.commissioning_serial_no_agreement_no like '%=%'
             and p.treatment_function_code = '320')
            or ({{ sus_op_rule_code_matches('NC_NWL', 'site_code', 'p.site_code_of_treatment', 'p.appointment_date') }}
                and p.commissioning_serial_no_agreement_no like '%=%'
                and p.treatment_function_code = '320')
        ) as rule_nc_nwl2,
        (
            ({{ sus_op_rule_code_matches('NC_NWL', 'provider_code', "left(p.organisation_code_code_of_provider, 3)", 'p.appointment_date') }}
             and p.commissioning_serial_no_agreement_no like '%=%'
             and p.treatment_function_code = '340')
            or ({{ sus_op_rule_code_matches('NC_NWL', 'site_code', 'p.site_code_of_treatment', 'p.appointment_date') }}
                and p.commissioning_serial_no_agreement_no like '%=%'
                and p.treatment_function_code = '340')
        ) as rule_nc_nwl3,
        (
            ({{ sus_op_rule_code_matches('NC_NWL', 'provider_code', "left(p.organisation_code_code_of_provider, 3)", 'p.appointment_date') }}
             and left(p.treatment_function_code, 2) = '14')
            or ({{ sus_op_rule_code_matches('NC_NWL', 'site_code', 'p.site_code_of_treatment', 'p.appointment_date') }}
                and left(p.treatment_function_code, 2) = '14')
        ) as rule_nc_nwl4,
        (
            ({{ sus_op_rule_code_matches('NC_NWL', 'provider_code', "left(p.organisation_code_code_of_provider, 3)", 'p.appointment_date') }}
             and left(p.treatment_function_code, 1) = '7')
            or ({{ sus_op_rule_code_matches('NC_NWL', 'site_code', 'p.site_code_of_treatment', 'p.appointment_date') }}
                and left(p.treatment_function_code, 1) = '7')
        ) as rule_nc_nwl5,
        (
            ({{ sus_op_rule_code_matches('NC_NWL', 'provider_code', "left(p.organisation_code_code_of_provider, 3)", 'p.appointment_date') }}
             and p.treatment_function_code = '310'
             and p.provider_reference_no is not null
             and not {{ sus_op_rule_code_matches('NC_NWL6', 'provider_reference_no', 'p.provider_reference_no', 'p.appointment_date', 'EXCLUDE') }})
            or ({{ sus_op_rule_code_matches('NC_NWL', 'site_code', 'p.site_code_of_treatment', 'p.appointment_date') }}
                and p.treatment_function_code = '310'
                and p.provider_reference_no is not null
                and not {{ sus_op_rule_code_matches('NC_NWL6', 'provider_reference_no', 'p.provider_reference_no', 'p.appointment_date', 'EXCLUDE') }})
        ) as rule_nc_nwl6,
        (
            ({{ sus_op_rule_code_matches('NC_NWL', 'provider_code', "left(p.organisation_code_code_of_provider, 3)", 'p.appointment_date') }}
             and p.treatment_function_code = '301'
             and {{ sus_op_rule_code_matches('NC_NWL7', 'provider_reference_no', 'p.provider_reference_no', 'p.appointment_date') }})
            or ({{ sus_op_rule_code_matches('NC_NWL', 'site_code', 'p.site_code_of_treatment', 'p.appointment_date') }}
                and p.treatment_function_code = '301'
                and {{ sus_op_rule_code_matches('NC_NWL7', 'provider_reference_no', 'p.provider_reference_no', 'p.appointment_date') }})
        ) as rule_nc_nwl7,
        p.commissioner_reference_no = 'S6002'
            and {{ sus_op_rule_code_matches('OPTH_BRENT', 'provider_code', "left(p.organisation_code_code_of_provider, 3)", 'p.appointment_date') }}
            and {{ sus_op_rule_code_matches('OPTH_BRENT', 'treatment_function_code', 'p.treatment_function_code', 'p.appointment_date') }}
            and {{ sus_op_rule_code_matches('OPTH_BRENT', 'commissioner_code', 'p.organisation_code_code_of_commissioner', 'p.appointment_date') }} as rule_opth_brent,
        coalesce(p.administrative_category, '00') in ('02', '2') as rule_private,
        {{ sus_op_rule_code_matches('SLEEPCLINIC_WMUH', 'provider_reference_no', 'p.provider_reference_no', 'p.appointment_date') }}
            and (
                {{ sus_op_rule_code_matches('WMUH_DIRECT', 'provider_code', "left(p.organisation_code_code_of_provider, 3)", 'p.appointment_date') }}
                or (
                    {{ sus_op_rule_code_matches('WMUH_SITE', 'provider_code', "left(p.organisation_code_code_of_provider, 3)", 'p.appointment_date') }}
                    and {{ sus_op_rule_code_matches('WMUH_SITE', 'provider_site_code', 'p.provider_site_code', 'p.appointment_date') }}
                )
            ) as rule_sleepclinic_wmuh,
        {{ sus_op_rule_code_matches('SLEEPSTUDY_WMUH', 'provider_reference_no', 'p.provider_reference_no', 'p.appointment_date') }}
            and (
                {{ sus_op_rule_code_matches('WMUH_DIRECT', 'provider_code', "left(p.organisation_code_code_of_provider, 3)", 'p.appointment_date') }}
                or (
                    {{ sus_op_rule_code_matches('WMUH_SITE', 'provider_code', "left(p.organisation_code_code_of_provider, 3)", 'p.appointment_date') }}
                    and {{ sus_op_rule_code_matches('WMUH_SITE', 'provider_site_code', 'p.provider_site_code', 'p.appointment_date') }}
                )
            ) as rule_sleepstudy_wmuh,
        {{ sus_op_rule_code_matches('SOAEC_WMUH', 'provider_reference_no', 'p.provider_reference_no', 'p.appointment_date') }}
            and (
                {{ sus_op_rule_code_matches('WMUH_DIRECT', 'provider_code', "left(p.organisation_code_code_of_provider, 3)", 'p.appointment_date') }}
                or (
                    {{ sus_op_rule_code_matches('WMUH_SITE', 'provider_code', "left(p.organisation_code_code_of_provider, 3)", 'p.appointment_date') }}
                    and {{ sus_op_rule_code_matches('WMUH_SITE', 'provider_site_code', 'p.provider_site_code', 'p.appointment_date') }}
                )
            ) as rule_soaec_wmuh
    from postprocessed as p
    left join rule_code_matches as matched
        on p.sk_encounter_id = matched.sk_encounter_id
),

with_sla as (
    select
        r.*,
        sla.commissioner_code is not null as has_sla
    from rule_flags as r
    left join {{ ref('sus_op_business_rules_sla') }} as sla
        on left(r.organisation_code_code_of_commissioner, 3) = sla.commissioner_code
       and left(r.organisation_code_code_of_provider, 3) = sla.provider_code
       and try_to_number(r.zfinancialyear) = sla.financial_year
)

select
    s.* exclude (
        has_sla,
        rule_aecu_clinic_lnwht, rule_aecu_wa_lnwht, rule_card_brent, rule_card_icht,
        rule_derm_cw, rule_dup_icht, rule_duplicate_lnwht, rule_ecg_cw, rule_gum,
        rule_gyn_cw, rule_in_health, rule_mh_london_providers, rule_mh_psych,
        rule_nc_nwl1, rule_nc_nwl2, rule_nc_nwl3, rule_nc_nwl4, rule_nc_nwl5,
        rule_nc_nwl6, rule_nc_nwl7, rule_opth_brent, rule_private,
        rule_sleepclinic_wmuh, rule_sleepstudy_wmuh, rule_soaec_wmuh
    )
    replace (
        {{ sus_op_business_rule_string('s.') }} as zbusinessrule,
        {{ sus_op_contract_type('s.') }} as zcontracttype
    )
from with_sla as s
