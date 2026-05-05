with inclusion_list as (
    select *
    from {{ ref('cltcs_full_detailed_patient_list') }}
),

encoding_features as (
    select
        il.patient_id,
        il.area_code,
        pd.age,
        zeroifnull(pc.total_conditions) as total_conditions,
        zeroifnull(pc.geriatric_conditions) as geriatric_conditions,
        zeroifnull(pc.neurology_conditions) as neurology_conditions,
        zeroifnull(pc.musculoskeletal_conditions) as musculoskeletal_conditions,
        case when pc.has_frailty = true then 1 else 0 end as has_frailty_flag,
        case when pc.has_dementia = true then 1 else 0 end as has_dementia_flag,
        case when pc.has_osteoporosis = true then 1 else 0 end as has_osteoporosis_flag,
        case when pc.has_parkinsons = true then 1 else 0 end as has_parkinsons_flag,
        case when pc.has_stroke_tia = true then 1 else 0 end as has_stroke_tia_flag,
        zeroifnull(aea.ae_tot_12mo) as ae_tot_12mo,
        zeroifnull(aea.ae_inj_12mo) as ae_inj_12mo,
        zeroifnull(apca.apc_nel_12mo) as apc_nel_12mo,
        zeroifnull(apca.apc_los_12mo) as apc_los_12mo,
        case
            when bp.latest_bp_date between dateadd(month, -6, current_date()) and current_date()
                then bp.is_overall_bp_controlled
            else null
        end as is_recent_bp_controlled,
        case
            when bp.latest_bp_date between dateadd(month, -6, current_date()) and current_date()
                and bp.is_overall_bp_controlled = false then 1
            else 0
        end as poor_recent_bp_control_flag,
        zeroifnull(ccms.cambridge_comorbidity_score) as cambridge_comorbidity_score,
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
        end as frailty_severity_weight
    from inclusion_list il
    left join {{ ref('dim_person_demographics') }} pd
        on il.olids_id = pd.person_id
    left join {{ ref('dim_person_conditions') }} pc
        on il.olids_id = pc.person_id
    left join {{ ref('fct_person_sus_ae_recent') }} aea
        on il.patient_id = aea.sk_patient_id
    left join {{ ref('fct_person_sus_ip_recent') }} apca
        on il.patient_id = apca.sk_patient_id
    left join {{ ref('fct_person_bp_control') }} bp
        on il.olids_id = bp.person_id
    left join {{ ref('stg_aic_int_ccms_current') }} ccms
        on il.olids_id = ccms.person_id
    left join {{ ref('stg_aic_int_efi2_scores') }} efi
        on il.olids_id = efi.person_id
    left join {{ ref('fct_person_frailty_register') }} fr
        on il.olids_id = fr.person_id
    left join {{ ref('int_rockwood_latest') }} rockwood
        on il.olids_id = rockwood.person_id
)

select
    patient_id,
    area_code,
    (
        greatest(age - 65, 0) / 5
        + has_frailty_flag * 8
        + has_dementia_flag * 6
        + has_osteoporosis_flag * 4
        + has_parkinsons_flag * 5
        + has_stroke_tia_flag * 4
        + total_conditions / 2
        + geriatric_conditions * 2
        + neurology_conditions * 2
        + musculoskeletal_conditions
        + ae_tot_12mo
        + ae_inj_12mo * 2
        + apc_nel_12mo * 3
        + apc_los_12mo / 4
        + poor_recent_bp_control_flag * 2
        + cambridge_comorbidity_score / 2
        + efi2_score * 10
        + efi2_category_weight * 3
        + rockwood_score * 2
        + frailty_severity_weight * 3
    ) as score_frailty
from encoding_features
