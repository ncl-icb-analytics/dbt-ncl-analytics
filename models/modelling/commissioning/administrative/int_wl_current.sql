{{
    config(
        materialized='view')
}}


/*
All active waiting lists for most recent census date from WLMDS.

Clinical Purpose:
- Care coordination management across providers

Includes ALL persons (active, inactive, deceased) following intermediate layer principles.

*/

WITH most_recent_week AS (
    SELECT MAX(week_ending_date) AS max_date
    FROM {{ ref('stg_wl_wl_openpathways_data') }}
)

SELECT
    week_ending_date,
    sk_patient_id,
    local_patient_identifier,
    person_stated_gender_code,
    ethnic_category,
    lsoa_2021,
    age_at_week_ending_date,
    age_at_referral_to_treatment_period_start_date,
    age_band_week_ending_date,
    age_band_at_referral_to_treatment_period_start_date,
    commissioner_code,
    practice_code,
    ccg_of_practice,
    ccg_of_residence,
    waiting_list_type,
    provider_code,
    provider_site_code,
    referring_organisation_code,
    referral_request_received_date,
    original_referral_request_received_date,
    referral_to_treatment_period_start_date,
    current_pathway_period_start_date,
    referral_identifier,
    patient_pathway_identifier,
    organisation_code_patient_pathway_identifier_issuer,
    source_of_referral,
    main_specialty_code,
    treatment_function_code,
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
    admission_method_code,
    priority_type_code,
    procedure_priority_code,
    diagnostic_priority_code,
    inclusion_on_cancer_ptl,
    intended_management_code,
    transfer_status,
    mutual_aid_accepting_provider,
    first_activity_date,
    first_activity_type,
    decision_to_treat_date,
    tci_date_provided,
    preliminary_screening_and_risk_assessment_date,
    date_and_time_data_set_created,
    1 as open_pathways
FROM {{ ref('stg_wl_wl_openpathways_data') }} wl
INNER JOIN most_recent_week mrw ON wl.week_ending_date = mrw.max_date
