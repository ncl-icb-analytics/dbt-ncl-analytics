          
with inclusion_list as (
    select patient_id, area_code, olids_id
    from {{ ref('cltcs_patient_list')}}
    ), 
    
encoding_features as(
    select il.patient_id
        , il.area_code
        , pd.age
        -- biomarker and monitoring gaps 
        ---- blood pressure
        , case when bp.latest_bp_date between dateadd(month, -6, current_date()) and current_date() and bp.is_overall_bp_controlled = false then 1 else 0 end as poor_recent_bp_control_flag
        , case when bp.latest_bp_date < dateadd(month, -12, current_date()) or bp.latest_bp_date is null and hpr.person_id is not null then 1 else 0 end as bp_monitoring_overdue_flag
        ---- hba1c 
        , case when hba1c.clinical_effective_date between dateadd(month, -6, current_date()) and current_date() and hba1c.meets_qof_target = false then 1 else 0 end as poor_recent_hba1c_control_flag
        , case when dpr.person_id is not null and dcp.hba1c_completed_in_last_12m = false then 1 else 0 end as hba1c_monitoring_overdue_flag
        , case when hba1c.hba1c_ifcc > 90 then 1 else 0 end as hba1c_dangerous_flag        
        -- ckd and egfr (intervals by latest eGFR mL/min/1.73m²: 45-59 yearly, 30-44 6-monthly, <30 ~quarterly)
        , case
            when eg.clinical_effective_date is null then 0
            when eg.egfr_value is null then 0
            when eg.egfr_value >= 45 and eg.egfr_value < 60
                and eg.clinical_effective_date < dateadd(month, -12, current_date()) then 1
            when eg.egfr_value >= 30 and eg.egfr_value < 45
                and eg.clinical_effective_date < dateadd(month, -6, current_date()) then 1
            when eg.egfr_value >= 0 and eg.egfr_value < 30
                and eg.clinical_effective_date < dateadd(month, -3, current_date()) then 1
            else 0
          end as egfr_overdue_flag

        -- actionable risk factors
        , zeroifnull(br.smoking_risk_sort_key) as smoking_risk_sort_key
        , zeroifnull(br.bmi_risk_sort_key) as bmi_risk_sort_key
        , zeroifnull(br.alcohol_risk_sort_key) as alcohol_risk_sort_key
        , zeroifnull(br.risk_factor_count) as risk_factor_count
        , case when id.illicit_drug_pattern is not null
                  and id.illicit_drug_pattern <> 'Does not misuse drugs' then 1 else 0 end as substance_misuse_flag

        -- medication burden
        , zeroifnull(rm.unique_active_ingredient_count_12mo) as unique_active_ingredient_count_12mo
        , case when polyp.is_polypharmacy_5plus then 1 else 0 end as polypharmacy_5plus_flag
        , case when polyp.is_polypharmacy_10plus then 1 else 0 end as polypharmacy_10plus_flag
        , zeroifnull(polyp.medication_count) as medication_count
        
        -- multimorbidity and biopsychosocial complexity
        , pc.total_qof_conditions
        , pc.mental_health_conditions
        , zeroifnull(ccms.cambridge_comorbidity_score) as cambridge_comorbidity_score
        , {{ encode_ltc_lcs_risk_group('lcs.chd_risk_group') }} as chd_risk_group_sort_key
        , {{ encode_ltc_lcs_risk_group('lcs.ckd_risk_group') }} as ckd_risk_group_sort_key
        , {{ encode_ltc_lcs_risk_group('lcs.copd_risk_group') }} as copd_risk_group_sort_key
        , {{ encode_ltc_lcs_risk_group('lcs.diabetes_risk_group') }} as diabetes_risk_group_sort_key
        , {{ encode_ltc_lcs_risk_group('lcs.hf_risk_group') }} as hf_risk_group_sort_key
        , {{ encode_ltc_lcs_risk_group('lcs.hypertension_risk_group') }} as hypertension_risk_group_sort_key
        , {{ encode_ltc_lcs_risk_group('lcs.overall_risk_group') }} as overall_risk_group_sort_key
        
        -- barriers to treatment
        , case when ps.is_housebound then 1 else 0 end as is_housebound_flag
        , ps.behavioural_risk_count
        , case when pc.has_severe_mental_illness then 1 else 0 end as has_smi_flag
        
        -- acute use related features
        , zeroifnull(aea.ae_ill_12mo) as ae_ill_12mo
        , zeroifnull(aea.ae_t1_12mo) as ae_t1_12mo
        , zeroifnull(gpa.gp_att_tot_12mo) as gp_att_tot_12mo
        , zeroifnull(aea.ae_lower_respiratory_attendance_12mo) as ae_lower_respiratory_attendance_12mo
        -- condition specific care gaps
        ---- asthma
        , case when am.testing_no_diagnosis = TRUE then 1 else 0 end as asthma_testing_no_diagnosis_flag
        , case when am.diagnosis_no_testing = TRUE then 1 else 0 end as asthma_diagnosis_no_testing_flag
        , case when am.diagnosis_no_act = TRUE then 1 else 0 end as asthma_diagnosis_no_act_flag
        , case when  am.salbutamol_only= TRUE then 1 else 0 end as asthma_salbutamol_only_flag
        , case when  am.salbutamol_repeats  = TRUE then 1 else 0 end as asthma_salbutamol_repeats_flag
        ---- Diabetes
        , case when pc.has_diabetes then greatest(0, 8 - zeroifnull(dcp.care_processes_completed)) else 0 end as diabetes_care_gap_count
        , case when pc.has_diabetes and coalesce(dt.all_three_targets_met, false) = false then 1 else 0 end as diabetes_not_triple_target_flag
        ---- COPD
        ---- chronic kidney disease
        ---- chronic heart disease
        -- Engagement in existing care pathways
        , lcs.moc_stage_completed
      from inclusion_list il
    left join {{ref('dim_person_conditions')}} pc
        on il.olids_id = pc.person_id
    left join {{ref('fct_person_sus_ae_recent')}} aea
        on il.patient_id  = aea.sk_patient_id
    left join {{ref('fct_person_gp_recent')}} gpa
        on il.patient_id  = gpa.sk_patient_id
    left join {{ref('fct_person_medications_recent')}} rm
        on il.olids_id = rm.person_id
    left join {{ref('dim_person_demographics')}} pd
        on il.olids_id = pd.person_id
    left join {{ref('fct_person_behavioural_risk_factors')}} br
        on il.olids_id = br.person_id
    left join {{ref('int_asthma_management')}} am
        on il.olids_id = am.person_id
    left join {{ref('fct_person_bp_control')}} bp
        on il.olids_id = bp.person_id
    left join {{ref('dim_person_ccms')}} ccms
        on il.olids_id = ccms.person_id
    left join {{ref('fct_person_ltc_lcs_risk_summary')}} lcs
        on il.olids_id = lcs.person_id
    left join {{ref('fct_person_polypharmacy_current')}} polyp
        on il.olids_id = polyp.person_id
    left join {{ref('fct_person_diabetes_register')}} dpr
        on il.olids_id = dpr.person_id
    left join {{ref('fct_person_hypertension_register')}} hpr
        on il.olids_id = hpr.person_id
    left join {{ref('fct_person_diabetes_8_care_processes')}} dcp
        on il.olids_id = dcp.person_id
    left join {{ref('fct_person_diabetes_triple_target')}} dt
        on il.olids_id = dt.person_id
    left join {{ref('int_hba1c_latest')}} hba1c
        on il.olids_id = hba1c.person_id
    left join {{ref('int_smi_lifestyle')}} id
        on il.olids_id = id.person_id
    left join {{ ref('int_egfr_latest') }} eg
        on il.olids_id = eg.person_id
    left join {{ref('dim_person_status_summary')}} ps
        on il.olids_id = ps.person_id
),

domain_sub_scores as (
    select
        patient_id,
        area_code,
        age,

        -- biomarker and monitoring gaps
        (
            poor_recent_bp_control_flag * 2
            + poor_recent_hba1c_control_flag *2
            + hba1c_dangerous_flag * 4
            + bp_monitoring_overdue_flag
            + hba1c_monitoring_overdue_flag
            + egfr_overdue_flag

        ) as score_biomarker_gaps,

        -- condition and prevention care gaps (overall LTC LCS risk only — avoid summing per-condition keys)
        (
            asthma_salbutamol_only_flag
            + asthma_salbutamol_repeats_flag
            + diabetes_care_gap_count
            + diabetes_not_triple_target_flag
            + case
                when zeroifnull(moc_stage_completed) < 2
                    and overall_risk_group_sort_key > 0 then 1
                else 0
              end * 2
            + risk_factor_count
            + smoking_risk_sort_key
            + bmi_risk_sort_key
            + alcohol_risk_sort_key
        ) as score_care_gaps,

        -- clinical complexity: multimorbidity 
        (
            chd_risk_group_sort_key
            + ckd_risk_group_sort_key
            + copd_risk_group_sort_key
            + diabetes_risk_group_sort_key
            + hf_risk_group_sort_key
            + hypertension_risk_group_sort_key
            + least(zeroifnull(total_qof_conditions), 8)
            + zeroifnull(mental_health_conditions)
            + cambridge_comorbidity_score * 5
        ) as score_complexity,

        -- D4: medication burden
        ( ln( 1+ medication_count)
            + ln( 1+ unique_active_ingredient_count_12mo) 
        ) as score_medication,

        -- illness-related UEC (not injury totals; no ae_ill_3mo in feature set)
        (
            ln( 1+ ae_ill_12mo)
            + ln( 1+ ae_t1_12mo) * 2
            + ln( 1+ ae_lower_respiratory_attendance_12mo)
            - ln( 1+ gp_att_tot_12mo)
        ) as score_illness_uec,
        -- barriers to treatment / and biopsychosocial treatment complexity
        (
            is_housebound_flag
            + behavioural_risk_count
            + has_smi_flag
            + substance_misuse_flag
        ) as score_barriers
        -- to add existring engagement in care pathways
    from encoding_features
),

composite_scores as (
    select
        patient_id,
        area_code,
        age,
        score_biomarker_gaps,
        score_care_gaps,
        score_complexity,
        score_medication,
        score_illness_uec,
        score_barriers,
        (score_biomarker_gaps - avg(score_biomarker_gaps) over (partition by area_code)) / nullif(stddev(score_biomarker_gaps) over (partition by area_code), 0) as scaled_score_biomarker_gaps,
        (score_care_gaps - avg(score_care_gaps) over (partition by area_code)) / nullif(stddev(score_care_gaps) over (partition by area_code), 0) as scaled_score_care_gaps,
        (score_complexity - avg(score_complexity) over (partition by area_code)) / nullif(stddev(score_complexity) over (partition by area_code), 0) as scaled_score_complexity,
        (score_medication - avg(score_medication) over (partition by area_code)) / nullif(stddev(score_medication) over (partition by area_code), 0) as scaled_score_medication,
        (score_illness_uec - avg(score_illness_uec) over (partition by area_code)) / nullif(stddev(score_illness_uec) over (partition by area_code), 0) as scaled_score_illness_uec,
        (score_barriers - avg(score_barriers) over (partition by area_code)) / nullif(stddev(score_barriers) over (partition by area_code), 0) as scaled_score_barriers
    from domain_sub_scores
),


clipped_scores as (
    select
        *,
        least(greatest(zeroifnull(scaled_score_biomarker_gaps), -3), 3) as clipped_score_biomarker_gaps,
        least(greatest(zeroifnull(scaled_score_care_gaps), -3), 3) as clipped_score_care_gaps,
        least(greatest(zeroifnull(scaled_score_complexity), -3), 3) as clipped_score_complexity,
        least(greatest(zeroifnull(scaled_score_medication), -3), 3) as clipped_score_medication,
        least(greatest(zeroifnull(scaled_score_illness_uec), -3), 3) as clipped_score_illness_uec,
        least(greatest(zeroifnull(scaled_score_barriers), -3), 3) as clipped_score_barriers,
        from composite_scores
),
remapped_scores as (
    select
        *,
        round((clipped_score_biomarker_gaps + 3) / 6.0 * 100, 1) as score_biomarker_gaps_0_100,
        round((clipped_score_care_gaps + 3) / 6.0 * 100, 1) as score_care_gaps_0_100,
        round((clipped_score_complexity + 3) / 6.0 * 100, 1) as score_complexity_0_100,
        round((clipped_score_medication + 3) / 6.0 * 100, 1) as score_medication_0_100,
        round((clipped_score_illness_uec + 3) / 6.0 * 100, 1) as score_illness_uec_0_100,
        round((clipped_score_barriers + 3) / 6.0 * 100, 1) as score_barriers_0_100,
        (score_biomarker_gaps_0_100 * 1 + score_care_gaps_0_100 * 1 + score_complexity_0_100 * 1 + score_medication_0_100 * 1 + score_illness_uec_0_100 * 1 + score_barriers_0_100 * 1) /6 as raw_score_treatment
    from clipped_scores
)

select
    *,
    raw_score_treatment * (
        case
            when age is null then 1.0
            else (
                1
                -- max +15% boost at age 18, linear decline to 0 at age 60
                + 0.15 * (60 - least(greatest(age, 18), 60)) / 42.0
                -- linear penalty from age 60, capped at -15% from age 100 onward
                - 0.15 * least(greatest(age - 60, 0), 40) / 40.0
            )
        end
    ) as score_treatment
from remapped_scores