with inclusion_list as (
    select patient_id, area_code, olids_id
    from {{ ref('cltcs_patient_list')}}
),

encoding_features as (
    select
        il.patient_id,
        il.area_code,
        pd.age,
        
        -- multimorbidity and biopsychosocial complexity
        pc.total_qof_conditions,
        pc.mental_health_conditions,
        zeroifnull(ccms.cambridge_comorbidity_score) as cambridge_comorbidity_score,
        {{ encode_ltc_lcs_risk_group('lcs.chd_risk_group') }} as chd_risk_group_sort_key,
        {{ encode_ltc_lcs_risk_group('lcs.ckd_risk_group') }} as ckd_risk_group_sort_key,
        {{ encode_ltc_lcs_risk_group('lcs.copd_risk_group') }} as copd_risk_group_sort_key,
        {{ encode_ltc_lcs_risk_group('lcs.diabetes_risk_group') }} as diabetes_risk_group_sort_key,
        {{ encode_ltc_lcs_risk_group('lcs.hf_risk_group') }} as hf_risk_group_sort_key,
        {{ encode_ltc_lcs_risk_group('lcs.hypertension_risk_group') }} as hypertension_risk_group_sort_key,
        {{ encode_ltc_lcs_risk_group('lcs.overall_risk_group') }} as overall_risk_group_sort_key,
        
        -- frailty
        zeroifnull(pc.geriatric_conditions) as geriatric_conditions,
        zeroifnull(pc.neurology_conditions) as neurology_conditions,
        zeroifnull(pc.musculoskeletal_conditions) as musculoskeletal_conditions,
        case when pc.has_frailty = true then 1 else 0 end as has_frailty_flag,
        case when pc.has_dementia = true then 1 else 0 end as has_dementia_flag,
        case when pc.has_osteoporosis = true then 1 else 0 end as has_osteoporosis_flag,
        case when pc.has_parkinsons = true then 1 else 0 end as has_parkinsons_flag,
        case when pc.has_stroke_tia = true then 1 else 0 end as has_stroke_tia_flag,
        zeroifnull(efi.efi_score) as efi2_score,
        case
            when efi.category in ('Severe frailty', 'Very severe frailty') then 3
            when efi.category = 'Moderate frailty' then 2
            when efi.category = 'Mild frailty' then 1
            else 0
        end as efi2_category_weight,
        zeroifnull(rockwood.rockwood_score) as rockwood_score,
        case
            when rockwood.rockwood_score >= 7 then 3
            when rockwood.rockwood_score >= 5 then 2
            when rockwood.rockwood_score >= 3 then 1
            else 0
        end as rockwood_category_weight,
        fr.latest_frailty_severity as frailty_severity,
        case when fr.latest_frailty_severity = 'Severe' then 3
            when fr.latest_frailty_severity = 'Moderate' then 2
            when fr.latest_frailty_severity = 'Mild' then 1
            else 0
        end as frailty_severity_weight,
        
        -- medicines management
        case when polyp.is_polypharmacy_5plus then 1 else 0 end as polypharmacy_5plus_flag,
        case when polyp.is_polypharmacy_10plus then 1 else 0 end as polypharmacy_10plus_flag,
        zeroifnull(polyp.medication_count) as medication_count,
        
        -- emergency use
        zeroifnull(aea.ae_tot_12mo) as ae_tot_12mo,
        zeroifnull(aea.ae_inj_12mo) as ae_inj_12mo,
        zeroifnull(apca.apc_nel_12mo) as apc_nel_12mo,
        zeroifnull(apca.apc_los_12mo) as apc_los_12mo,
        zeroifnull(apca.acs_nel_12mo) as acs_nel_12mo,

        -- residential / social factors
        case when ch.is_care_home_resident = TRUE then 1 else 0 end as is_care_home_flag,
        case when pc.has_palliative_care = true then 1 else 0 end as is_palliative_care_flag,
        case when ps.is_housebound = true then 1 else 0 end as is_housebound_flag,
        case when pc.has_learning_disability = true then 1 else 0 end as has_learning_disability_flag,
        case when pc.has_severe_mental_illness = true then 1 else 0 end as has_severe_mental_illness_flag,
        case when id.illicit_drug_pattern is not null
                  and id.illicit_drug_pattern <> 'Does not misuse drugs' then 1 else 0 end as substance_misuse_flag,
        -- wider care engagement
        asc_cld.primary_support_reason_category_count,
        case when asc_cld.has_physical_support_personal_care = true then 1 else 0 end as has_physical_support_personal_care_flag,
        case when asc_cld.has_physical_support_access_mobility = true then 1 else 0 end as has_physical_support_access_mobility_flag,
        case when asc_cld.has_memory_cognition_support = true then 1 else 0 end as has_memory_cognition_support_flag,
        case when asc_cld.has_social_support_unpaid_carer = true then 1 else 0 end as has_social_support_unpaid_carer_flag,
        case when asc_cld.has_social_support_social_isolation = true then 1 else 0 end as has_social_support_social_isolation_flag,
        case when asc_cld.has_sensory_support_visual_impairment = true then 1 else 0 end as has_sensory_support_visual_impairment_flag,
        case when asc_cld.has_sensory_support_hearing_impairment = true then 1 else 0 end as has_sensory_support_hearing_impairment_flag,
        case when asc_cld.has_sensory_support_dual_impairment = true then 1 else 0 end as has_sensory_support_dual_impairment_flag,
       
    from inclusion_list il
    left join {{ ref('dim_person_demographics') }} pd
        on il.olids_id = pd.person_id
    left join {{ ref('dim_person_conditions') }} pc
        on il.olids_id = pc.person_id
    left join {{ref('dim_person_status_summary')}} ps
        on il.olids_id = ps.person_id
    left join {{ref('fct_person_ltc_lcs_risk_summary')}} lcs
        on il.olids_id = lcs.person_id
    left join {{ ref('fct_person_sus_ae_recent') }} aea
        on il.patient_id = aea.sk_patient_id
    left join {{ ref('fct_person_sus_ip_recent') }} apca
        on il.patient_id = apca.sk_patient_id
    left join {{ ref('fct_person_bp_control') }} bp
        on il.olids_id = bp.person_id
    left join {{ ref('dim_person_ccms') }} ccms
        on il.olids_id = ccms.person_id
    left join {{ ref('stg_aic_int_efi2_scores') }} efi
        on il.olids_id = efi.person_id
    left join {{ ref('fct_person_frailty_register') }} fr
        on il.olids_id = fr.person_id
    left join {{ ref('int_rockwood_latest') }} rockwood
        on il.olids_id = rockwood.person_id
    left join {{ ref('fct_person_polypharmacy_current') }} polyp
        on il.olids_id = polyp.person_id
    left join {{ ref('int_smi_illicit_drug_latest') }} id
        on il.olids_id = id.person_id
    left join {{ ref('dim_person_care_home') }} ch
        on il.olids_id = ch.person_id
    left join {{ref('fct_person_asc_service_recent')}} asc_cld
        on il.patient_id = asc_cld.sk_patient_id

),

domain_sub_scores as (
    select
        patient_id,
        area_code,
        age,
        -- clinical complexity / multimorbidity
        ( total_qof_conditions
        + mental_health_conditions
        + cambridge_comorbidity_score * 10
       + chd_risk_group_sort_key
       + ckd_risk_group_sort_key
       + copd_risk_group_sort_key
       + diabetes_risk_group_sort_key
       + hf_risk_group_sort_key
       + hypertension_risk_group_sort_key
       ) as score_clinical_complexity,
        -- clinical frailty
        ( greatest(frailty_severity_weight, rockwood_category_weight, efi2_category_weight) * 2
        + has_dementia_flag
        + has_osteoporosis_flag
        + has_parkinsons_flag
        + has_stroke_tia_flag
        + geriatric_conditions
        + neurology_conditions
        + musculoskeletal_conditions
        ) as score_clinical_frailty,
        -- medicines management
        ( polypharmacy_5plus_flag
        + polypharmacy_10plus_flag * 3
        + ln(1+medication_count)
        ) as score_medicines_management,
        -- emergency use
        ( ln(1+ae_tot_12mo)
        + ln(1+ae_inj_12mo)
        + ln(1+apc_nel_12mo)
            -- TO DO: add specific discharge reason flags (should be in ASC?)
        ) as score_emergency_use,
        -- residential / social factors
        ( is_palliative_care_flag
        + is_housebound_flag
        + has_learning_disability_flag
        + has_severe_mental_illness_flag
        ) as score_residential_social_factors,
        -- wider care engagement
        ( ln(1+apc_los_12mo)
        + has_physical_support_personal_care_flag
        + has_physical_support_access_mobility_flag
        + has_memory_cognition_support_flag
        + has_social_support_unpaid_carer_flag
        + has_social_support_social_isolation_flag
        + has_sensory_support_visual_impairment_flag
        + has_sensory_support_hearing_impairment_flag
        + has_sensory_support_dual_impairment_flag
        ) as score_wider_care_engagement,
            -- TO DO: add community, referrals and carer support flags
        -- ASC indicators
        ( ln(1+acs_nel_12mo)
        ) as score_asc_indicators,
        -- care home and other exclusions
        ( is_care_home_flag ) as score_exclusions
from encoding_features
),

composite_scores as (
    select
        patient_id,
        area_code,
        age,
        (score_clinical_complexity - avg(score_clinical_complexity) over (partition by area_code)) / nullif(stddev(score_clinical_complexity) over (partition by area_code), 0) as scaled_score_clinical_complexity,
        (score_clinical_frailty - avg(score_clinical_frailty) over (partition by area_code)) / nullif(stddev(score_clinical_frailty) over (partition by area_code), 0) as scaled_score_clinical_frailty,
        (score_medicines_management - avg(score_medicines_management) over (partition by area_code)) / nullif(stddev(score_medicines_management) over (partition by area_code), 0) as scaled_score_medicines_management,
        (score_emergency_use - avg(score_emergency_use) over (partition by area_code)) / nullif(stddev(score_emergency_use) over (partition by area_code), 0) as scaled_score_emergency_use,
        (score_residential_social_factors - avg(score_residential_social_factors) over (partition by area_code)) / nullif(stddev(score_residential_social_factors) over (partition by area_code), 0) as scaled_score_residential_social_factors,
        (score_wider_care_engagement - avg(score_wider_care_engagement) over (partition by area_code)) / nullif(stddev(score_wider_care_engagement) over (partition by area_code), 0) as scaled_score_wider_care_engagement,
        (score_asc_indicators - avg(score_asc_indicators) over (partition by area_code)) / nullif(stddev(score_asc_indicators) over (partition by area_code), 0) as scaled_score_asc_indicators,
        score_exclusions
    from domain_sub_scores ),

clipped_scores as (
    select
        *,
        least(greatest(zeroifnull(scaled_score_clinical_complexity), -3), 3) as clipped_score_clinical_complexity,
        least(greatest(zeroifnull(scaled_score_clinical_frailty), -3), 3) as clipped_score_clinical_frailty,
        least(greatest(zeroifnull(scaled_score_medicines_management), -3), 3) as clipped_score_medicines_management,
        least(greatest(zeroifnull(scaled_score_emergency_use), -3), 3) as clipped_score_emergency_use,
        least(greatest(zeroifnull(scaled_score_residential_social_factors), -3), 3) as clipped_score_residential_social_factors,
        least(greatest(zeroifnull(scaled_score_wider_care_engagement), -3), 3) as clipped_score_wider_care_engagement,
        least(greatest(zeroifnull(scaled_score_asc_indicators), -3), 3) as clipped_score_asc_indicators
    from composite_scores
),

reweighted_scores as (
    select *,
     ( 1 * clipped_score_clinical_complexity + 1 * clipped_score_clinical_frailty + 1 * clipped_score_medicines_management + 1 * clipped_score_emergency_use + 1 * clipped_score_residential_social_factors + 1 * clipped_score_wider_care_engagement + 1 * clipped_score_asc_indicators) / 7 as raw_score_frailty
    from clipped_scores
)
select *,
    greatest(0, ground((raw_score_frailty + 3) / 6.0 * 100, 1) - (50 * score_exclusions)) as score_frailty
from reweighted_scores
