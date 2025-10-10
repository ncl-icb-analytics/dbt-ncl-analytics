{{
    config(materialized = 'view')
}}

select  primarykey_id,
    appointment_identifier, 
    appointment_date,
    appointment_patient_identity_nhs_number_value_pseudo as sk_patient_id,
    appointment_commissioning_service_agreement_provider, 
    appointment_care_location_site_code_of_treatment,
    appointment_expected_duration,
    appointment_outcome,
    appointment_attended_or_dna,
    appointment_first_attendance,
    appointment_care_professional_main_specialty,
    appointment_referral_priority_type,
    appointment_care_professional_treatment_function,
    appointment_commissioning_grouping_core_hrg,
    appointment_commissioning_tariff_calculation_final_price
from {{ ref('raw_sus_op_appointment') }}