{{
    config(materialized = 'view')
}}

select primarykey_id
    , patient_nhs_number_value_pseudo as sk_patient_id
    , attendance_location_hes_provider_3 
    , attendance_location_site 
    , attendance_arrival_date 
    , attendance_departure_time_since_arrival 
    , clinical_chief_complaint_code 
    , clinical_acuity_code 
    , attendance_location_department_type 
    , commissioning_national_pricing_final_price 
from {{ ref('raw_sus_ae_emergency_care') }}