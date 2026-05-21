with emis as (
    select
        cast(sk_patient_id as varchar) as sk_patient_id,
        ltc_lcs,
        borough,
        patient_details_usual_gp_s_organisation_code,
        patient_details_age,
        patient_details_date_of_birth,
        patient_details_gender,
        patient_details_ethnic_origin,
        patient_details_townsend_score,
        patient_details_lower_layer_area_2001,
        patient_details_middle_layer_area_2001,
        patient_details_lower_layer_area_2011,
        patient_details_middle_layer_area_2011,
        record_of_interpreter_information_exists,
        homelessness_exists,
        housebound_exists,
        care_home_resident_exists,
        cardiovascular_disease_exists,
        atrial_fibrillation_exists,
        hypertension_exists,
        hyperlipidaemia_exists,
        diabetes_exists,
        chronic_kidney_disease_exists,
        na_fatty_liver_disease_exists,
        copd_exists,
        asthma_exists,
        serious_mi_exists,
        learning_disability_exists,
        frailty_exists,
        smoking_status_date,
        smoking_status_code_term,
        smoking_status_value,
        bmi_date,
        bmi_code_term,
        bmi_value,
        o_e_bp_reading_date,
        o_e_bp_reading_code_term,
        o_e_bp_reading_value,
        o_e_bp_reading_secondary_value,
        hba1_c_date,
        hba1_c_code_term,
        hba1_c_value,
        egfr_date,
        egfr_code_term,
        egfr_value,
        uacr_date,
        uacr_code_term,
        uacr_value,
        tc_date,
        tc_code_term,
        tc_value,
        ldl_date,
        ldl_code_term,
        ldl_value,
        moc_check_test_appointment_on_date,
        moc_follow_up_appointment_on_date,
        rpt_count_count,
        record_source,
        gp_practice_warning_flag,
        metadata_file_path,
        metadata_file_row_number,
        metadata_record_ingestion_timestamp,
        metadata_file_content_key,
        metadata_file_last_modified
    from {{ ref('raw_cltcs_emis_emis') }}
    where sk_patient_id is not null
    qualify row_number() over (
        partition by
            cast(sk_patient_id as varchar),
            patient_details_usual_gp_s_organisation_code
        order by
            metadata_record_ingestion_timestamp desc nulls last,
            metadata_file_last_modified desc nulls last,
            metadata_file_row_number desc nulls last
    ) = 1
),

registered_practice as (
    select
        cast(sk_patient_id as varchar) as sk_patient_id,
        practice_code
    from {{ ref('dim_person_demographics_basic') }}
    where practice_code is not null
),

with_pds as (
    select
        emis.*,
        reg.practice_code as registered_practice_code,
        emis.patient_details_usual_gp_s_organisation_code is distinct from reg.practice_code
            as registered_practice_mismatch_flag
    from emis
    left join registered_practice reg
        on emis.sk_patient_id = reg.sk_patient_id
)

select
    sk_patient_id,
    ltc_lcs,
    borough,
    patient_details_usual_gp_s_organisation_code as practice_code,
    registered_practice_code,
    registered_practice_mismatch_flag,
    patient_details_age as age,
    patient_details_date_of_birth as date_of_birth,
    patient_details_gender as gender,
    patient_details_ethnic_origin as ethnic_origin,
    patient_details_townsend_score as townsend_score,
    patient_details_lower_layer_area_2001 as lower_layer_area_2001,
    patient_details_middle_layer_area_2001 as middle_layer_area_2001,
    patient_details_lower_layer_area_2011 as lower_layer_area_2011,
    patient_details_middle_layer_area_2011 as middle_layer_area_2011,
    record_of_interpreter_information_exists as record_of_interpreter_information,
    homelessness_exists as homelessness,
    housebound_exists as housebound,
    care_home_resident_exists as care_home_resident,
    cardiovascular_disease_exists as cardiovascular_disease,
    atrial_fibrillation_exists as atrial_fibrillation,
    hypertension_exists as hypertension,
    hyperlipidaemia_exists as hyperlipidaemia,
    diabetes_exists as diabetes,
    chronic_kidney_disease_exists as chronic_kidney_disease,
    na_fatty_liver_disease_exists as na_fatty_liver_disease,
    copd_exists as copd,
    asthma_exists as asthma,
    serious_mi_exists as serious_mi,
    learning_disability_exists as learning_disability,
    frailty_exists as frailty,
    smoking_status_date,
    smoking_status_code_term as smoking_status_code,
    smoking_status_value,
    bmi_date,
    bmi_code_term as bmi_code,
    bmi_value,
    o_e_bp_reading_date,
    o_e_bp_reading_code_term as o_e_bp_reading_code,
    o_e_bp_reading_value,
    o_e_bp_reading_secondary_value,
    hba1_c_date,
    hba1_c_code_term as hba1_c_code,
    hba1_c_value,
    egfr_date,
    egfr_code_term as egfr_code,
    egfr_value,
    uacr_date,
    uacr_code_term as uacr_code,
    uacr_value,
    tc_date,
    tc_code_term as tc_code,
    tc_value,
    ldl_date,
    ldl_code_term as ldl_code,
    ldl_value,
    moc_check_test_appointment_on_date,
    moc_follow_up_appointment_on_date,
    rpt_count_count,
    record_source,
    gp_practice_warning_flag,
    metadata_file_path,
    metadata_file_row_number,
    metadata_record_ingestion_timestamp,
    metadata_file_content_key,
    metadata_file_last_modified
from with_pds
qualify
    count_if(not registered_practice_mismatch_flag) over (
        partition by sk_patient_id
    ) = 0
    or not registered_practice_mismatch_flag
