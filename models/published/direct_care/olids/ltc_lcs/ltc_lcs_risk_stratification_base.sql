{{
    config(
        materialized='table',
        cluster_by=['overall_risk_rank']
    )
}}

-- LTC LCS Risk Stratification Base Table
-- Combines the person-level risk summary with demographics and population health filter flags.
-- One row per person on the LTC LCS MOC base population.

select
    -- ============================================================
    -- Identifiers
    -- ============================================================
    d.person_id,
    d.sk_patient_id,
    pseudo.hx_flake,
    d.postcode_hash,
    d.uprn_hash,

    -- ============================================================
    -- Person status
    -- ============================================================
    d.is_active,
    d.is_deceased,

    -- ============================================================
    -- Demographics: core
    -- ============================================================
    d.gender,
    d.age,
    d.age_band_5y,
    d.age_band_10y,
    d.age_band_nhs,
    d.age_band_esp,
    d.age_life_stage,

    -- ============================================================
    -- Demographics: ethnicity
    -- ============================================================
    d.ethnicity_category,
    d.ethnicity_subcategory,
    d.ethnicity_granular,

    -- ============================================================
    -- Demographics: language
    -- ============================================================
    d.main_language,
    d.language_type,
    d.interpreter_needed,
    d.interpreter_type,

    -- ============================================================
    -- Geography: residence
    -- ============================================================
    d.lsoa_code_21 as lsoa_code,
    d.lsoa_name_21 as lsoa_name,
    d.ward_code,
    d.ward_name,
    d.borough_resident,
    d.neighbourhood_resident,
    d.icb_code_resident,
    d.icb_resident,

    -- ============================================================
    -- Geography: GP practice registration
    -- ============================================================
    d.practice_code,
    d.practice_name,
    d.pcn_code,
    d.pcn_name,
    d.borough_registered,
    d.neighbourhood_registered,

    -- ============================================================
    -- Deprivation
    -- ============================================================
    d.imd_decile_19,
    d.imd_quintile_19,

    -- ============================================================
    -- Age-standardisation weights (ESP 2013)
    -- ============================================================
    d.esp_weight,
    d.esp_proportion,

    -- ============================================================
    -- Population health inclusion filters
    -- ============================================================
    cond.has_learning_disability,
    cond.has_severe_mental_illness,
    coalesce(preg.is_currently_pregnant, false) as is_currently_pregnant,

    -- ============================================================
    -- Risk stratification: per-condition
    -- ============================================================
    rs.chd_risk_group,
    rs.ckd_risk_group,
    rs.copd_risk_group,
    rs.diabetes_risk_group,
    rs.hf_risk_group,
    rs.hypertension_risk_group,

    -- ============================================================
    -- Risk stratification: overall
    -- ============================================================
    rs.overall_risk_group,
    rs.overall_risk_rank,
    rs.in_any_risk_group,

    -- ============================================================
    -- MOC: pathway identity
    -- ============================================================
    rs.moc_pathway,

    -- ============================================================
    -- MOC: activity flags + dates (last 12 months), in pathway order
    -- ============================================================
    rs.moc_check_test_completed,
    rs.moc_check_test_date,
    rs.moc_mdt_review_completed,
    rs.moc_mdt_review_date,
    rs.moc_careplan_sharing_completed,
    rs.moc_careplan_sharing_date,
    rs.moc_stage_2_completed,
    rs.moc_stage_2_date,
    rs.moc_discussion_completed,
    rs.moc_discussion_date,
    rs.moc_followup_completed,
    rs.moc_followup_date,
    rs.moc_declined,
    rs.moc_declined_date,
    rs.moc_re_engaged_after_decline,
    rs.moc_any_activity_12m,

    -- ============================================================
    -- MOC: progression summary
    -- ============================================================
    rs.moc_stage_completed,
    rs.moc_stage_completed_label,
    rs.moc_pathway_status,
    rs.moc_next_action,
    rs.moc_cycle_complete,

    -- ============================================================
    -- MOC: data quality - missing prior stages
    -- ============================================================
    rs.moc_missing_check_test,
    rs.moc_missing_stage_2,
    rs.moc_missing_discussion,
    rs.moc_has_missing_priors,

    -- ============================================================
    -- MOC: stage durations (days)
    -- ============================================================
    rs.moc_days_check_test_to_stage_2,
    rs.moc_days_stage_2_to_discussion,
    rs.moc_days_discussion_to_followup,
    rs.moc_days_check_test_to_followup,

    -- ============================================================
    -- MOC: expiry dates
    -- ============================================================
    rs.moc_careplan_expires_date,
    rs.moc_next_expiry_date,

    -- ============================================================
    -- Metadata
    -- ============================================================
    rs.table_refresh_date
from {{ ref('fct_person_ltc_lcs_risk_summary') }} rs
inner join {{ ref('dim_person_demographics') }} d
    on rs.person_id = d.person_id
left join {{ ref('dim_person_pseudo') }} pseudo
    on rs.person_id = pseudo.person_id
left join {{ ref('dim_person_conditions') }} cond
    on rs.person_id = cond.person_id
left join {{ ref('fct_person_pregnancy_status') }} preg
    on rs.person_id = preg.person_id
