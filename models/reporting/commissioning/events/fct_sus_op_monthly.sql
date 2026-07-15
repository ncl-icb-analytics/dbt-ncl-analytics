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

care_home_assignment as (
    select
        b.sk_encounter_id,
        max_by(org.organisation_code, ch.period_start) as care_home_code
    from base as b
    inner join {{ ref('raw_fact_patient_factcarehome') }} as ch
        on b.sk_patient_id = ch.sk_patient_id
       and b.appointment_date between cast(ch.period_start as date)
                                  and coalesce(cast(ch.period_end as date), '2050-12-31'::date)
    left join {{ ref('stg_dictionary_dbo_organisation') }} as org
        on ch.sk_organisation_id = org.sk_organisation_id
    group by b.sk_encounter_id
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

postprocessed as (
    select
        d.* exclude (duplicate_rank)
        replace (
            iff(d.duplicate_rank > 1, 2, d.zcommissioning_access) as zcommissioning_access,
            coalesce(d.zsensitive_data_category, {{ sus_op_sensitive_category('d.') }})
                as zsensitive_data_category,
            coalesce(d.zpod_level_4, {{ sus_op_pod_level_4('d.') }}) as zpod_level_4,
            iff(d.tariff_total_payment_national is not null,
                d.tariff_total_payment_national, d.zderivedprice) as zderivedprice,
            iff(d.tariff_total_payment_national is not null, 'PBR', d.zderivedpriceflag)
                as zderivedpriceflag,
            iff(d.tariff_total_payment_national is not null, current_timestamp(), d.zderivedpricedt)
                as zderivedpricedt,
            d.local_patient_identifier as zlocalpatientidentifier,
            coalesce(d.zcarehome, ch.care_home_code) as zcarehome
        )
    from dedupe_ranked as d
    left join care_home_assignment as ch
        on d.sk_encounter_id = ch.sk_encounter_id
),

rule_flags as (
    select
        p.*,
        left(p.organisation_code_code_of_provider, 3) in ('RV8', 'R1K')
            and p.provider_reference_no = 'AMBUL17D' as rule_aecu_clinic_lnwht,
        left(p.organisation_code_code_of_provider, 3) in ('RV8', 'R1K')
            and p.provider_reference_no = 'AMBCAREWA' as rule_aecu_wa_lnwht,
        left(p.organisation_code_code_of_provider, 3) = 'RAL'
            and p.consultant_code in ('C4525871','C4191142','C4663760','C5207347','C4207100')
            and p.treatment_function_code = '320'
            and p.appointment_date >= '2015-03-01'::date
            and p.organisation_code_code_of_commissioner in ('5K5', '07P') as rule_card_brent,
        (
            p.provider_reference_no like '%QPC%'
            or p.provider_reference_no like '%WCC%'
            or (
                p.provider_reference_no in (
                    '09A08Y_320CD','09A08Y_320CFA','09A08Y_320CFU','09A08Y_320CHF','09A08Y_320CHFU'
                )
                and left(p.organisation_code_code_of_provider, 3) in ('RYJ','RQN','RJ5')
            )
        ) as rule_card_icht,
        p.site_code_of_treatment in ('RQM23', 'RQM20') as rule_derm_cw,
        p.provider_reference_no like '%DUP%'
            and left(p.organisation_code_code_of_provider, 3) in ('RYJ','RQN','RJ5') as rule_dup_icht,
        left(p.organisation_code_code_of_provider, 3) = 'RV8'
            and p.appointment_date between '2015-04-01'::date and '2015-04-30'::date
            as rule_duplicate_lnwht,
        p.treatment_function_code = '304'
            and left(p.organisation_code_code_of_provider, 3) = 'RQM' as rule_ecg_cw,
        p.treatment_function_code = '360' as rule_gum,
        p.site_code_of_treatment = 'RQM19' as rule_gyn_cw,
        left(p.organisation_code_code_of_provider, 3) = 'NV1' as rule_in_health,
        left(p.organisation_code_code_of_provider, 3)
            in ('RKL','RV3','RRP','RNK','RQY','RWK','TAF','RPG','RV5','RAT','RWR')
            as rule_mh_london_providers,
        left(p.treatment_function_code, 1) = '7' as rule_mh_psych,
        (
            (left(p.organisation_code_code_of_provider, 3) = 'RV8'
             and p.commissioning_serial_no_agreement_no = 'NCB')
            or (p.site_code_of_treatment = 'R1K01'
                and p.commissioning_serial_no_agreement_no = 'NCB')
        ) as rule_nc_nwl1,
        (
            (left(p.organisation_code_code_of_provider, 3) = 'RV8'
             and p.commissioning_serial_no_agreement_no like '%=%'
             and p.treatment_function_code = '320')
            or (p.site_code_of_treatment = 'R1K01'
                and p.commissioning_serial_no_agreement_no like '%=%'
                and p.treatment_function_code = '320')
        ) as rule_nc_nwl2,
        (
            (left(p.organisation_code_code_of_provider, 3) = 'RV8'
             and p.commissioning_serial_no_agreement_no like '%=%'
             and p.treatment_function_code = '340')
            or (p.site_code_of_treatment = 'R1K01'
                and p.commissioning_serial_no_agreement_no like '%=%'
                and p.treatment_function_code = '340')
        ) as rule_nc_nwl3,
        (
            (left(p.organisation_code_code_of_provider, 3) = 'RV8'
             and left(p.treatment_function_code, 2) = '14')
            or (p.site_code_of_treatment = 'R1K01'
                and left(p.treatment_function_code, 2) = '14')
        ) as rule_nc_nwl4,
        (
            (left(p.organisation_code_code_of_provider, 3) = 'RV8'
             and left(p.treatment_function_code, 1) = '7')
            or (p.site_code_of_treatment = 'R1K01'
                and left(p.treatment_function_code, 1) = '7')
        ) as rule_nc_nwl5,
        (
            (left(p.organisation_code_code_of_provider, 3) = 'RV8'
             and p.treatment_function_code = '310'
             and p.provider_reference_no not in ('CAUD15E','CAUDREP15E','NAUD15E','NAUDREP15E'))
            or (p.site_code_of_treatment = 'R1K01'
                and p.treatment_function_code = '310'
                and p.provider_reference_no not in ('CAUD15E','CAUDREP15E','NAUD15E','NAUDREP15E'))
        ) as rule_nc_nwl6,
        (
            (left(p.organisation_code_code_of_provider, 3) = 'RV8'
             and p.treatment_function_code = '301'
             and (p.provider_reference_no like 'NPBCS%' or p.provider_reference_no like '%breath%'))
            or (p.site_code_of_treatment = 'R1K01'
                and p.treatment_function_code = '301'
                and (p.provider_reference_no like 'NPBCS%' or p.provider_reference_no like '%breath%'))
        ) as rule_nc_nwl7,
        p.commissioner_reference_no = 'S6002'
            and left(p.organisation_code_code_of_provider, 3) = 'NT4'
            and p.treatment_function_code = '130'
            and p.appointment_date >= '2014-10-01'::date
            and p.organisation_code_code_of_commissioner in ('5K5', '07P') as rule_opth_brent,
        coalesce(p.administrative_category, '00') in ('02', '2') as rule_private,
        p.appointment_date >= '2012-04-01'::date
            and p.provider_reference_no = 'SLEEP CLIN'
            and (
                left(p.organisation_code_code_of_provider, 3) = 'RFW'
                or (left(p.organisation_code_code_of_provider, 3) = 'RQM'
                    and p.provider_site_code = 'RQM91')
            ) as rule_sleepclinic_wmuh,
        p.appointment_date >= '2012-04-01'::date
            and p.provider_reference_no = 'SLEEPSTYSC'
            and (
                left(p.organisation_code_code_of_provider, 3) = 'RFW'
                or (left(p.organisation_code_code_of_provider, 3) = 'RQM'
                    and p.provider_site_code = 'RQM91')
            ) as rule_sleepstudy_wmuh,
        p.appointment_date >= '2015-07-01'::date
            and p.provider_reference_no = 'SOAEC'
            and (
                left(p.organisation_code_code_of_provider, 3) = 'RFW'
                or (left(p.organisation_code_code_of_provider, 3) = 'RQM'
                    and p.provider_site_code = 'RQM91')
            ) as rule_soaec_wmuh
    from postprocessed as p
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
