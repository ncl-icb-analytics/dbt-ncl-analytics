{{
    config(materialized = 'view')
}}

SELECT
    CASE
        WHEN DAYOFWEEKISO(week_ending_date) = 7 THEN week_ending_date  -- Already Sunday
        ELSE DATEADD('day', -DAYOFWEEK(week_ending_date), week_ending_date)  -- Move to previous Sunday
    END AS week_ending_date,
    pseudo_nhs_number as sk_patient_id,
    local_patient_identifier,
    person_stated_gender_code,
    ethnic_category,
    der_lsoa2021 as lsoa_2021,
    der_age_week_ending_date as age_at_week_ending_date,
    der_age_at_referral_to_treatment_period_start_date as age_at_referral_to_treatment_period_start_date,
    der_age_band_week_ending_date as age_band_week_ending_date,
    der_age_band_at_referral_to_treatment_period_start_date as age_band_at_referral_to_treatment_period_start_date,
    organisation_identifier_code_of_commissioner as commissioner_code,
    der_practice_code as practice_code,
    der_ccg_of_practice as ccg_of_practice,
    der_ccg_of_residence as ccg_of_residence,
    waiting_list_type,
    organisation_identifier_code_of_provider as provider_code,
    organisation_site_identifier_of_treatment as provider_site_code,
    organisation_identifier_referring_organisation as referring_organisation_code,
    referral_request_received_date,
    original_referral_request_received_date,
    referral_to_treatment_period_start_date,
    current_pathway_period_start_date,
    referral_identifier,
    patient_pathway_identifier,
    organisation_code_patient_pathway_identifier_issuer,
    source_of_referral,
    main_specialty_code,
    activity_treatment_function_code as treatment_function_code,
    consultant_code,
    outpatient_future_appointment_date,
    due_date,
    outpatient_appointment_date,
    date_last_attended,
    last_dna_date,
    cancellation_date,
    outcome_of_attendance_code,
    tci_date,
    referral_to_treatment_period_status,
    decision_to_admit_date,
    proposed_procedure_opcs_code,
    admission_method_code_hospital_provider_spell as admission_method_code,
    priority_type_code,
    procedure_priority_code,
    diagnostic_priority_code,
    date_of_last_priority_review,
    last_pas_validation_date,
    inclusion_on_cancer_ptl,
    dm_icb_commissioner,
    dm_sub_icb_commissioner,
    intended_management_code,
    asa_physical_status_classification_system_code,
    transfer_status,
    mutual_aid_accepting_provider,
    first_activity_date,
    first_activity_type,
    decision_to_treat_date,
    tci_date_provided,
    preliminary_screening_and_risk_assessment_date,
    action_following_preliminary_screening_and_risk_assessment,
    date_and_time_data_set_created,
    der_submission_id as submission_id,
    der_row_id as row_id,
    1 as open_pathways
FROM {{ ref('raw_wl_wl_openpathways_data') }}
WHERE week_ending_date IS NOT NULL
    AND CASE
        WHEN DAYOFWEEKISO(week_ending_date) = 7 THEN week_ending_date  -- Already Sunday
        ELSE DATEADD('day', -DAYOFWEEK(week_ending_date), week_ending_date)  -- Move to previous Sunday
        END <= CURRENT_DATE
    AND referral_to_treatment_period_end_date IS NULL