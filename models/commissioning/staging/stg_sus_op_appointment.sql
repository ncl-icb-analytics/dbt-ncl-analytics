{{
    config(materialized = 'view')
}}

select  primarykey_id,
    appointment_date,
    appointment_patient_identity_nhs_number_value_pseudo, --as sk_patient_id,
    appointment_commissioning_service_agreement_provider, 
    appointment_care_location_site_code_of_treatment,
from {{ ref('raw_sus_op_appointment') }}