{{
    config(materialized = 'view')
}}

select primarykey_id
    , patient_nhs_number_value_pseudo as sk_patient_id
    , attendance_location_hes_provider_3 
    , attendance_location_site 
    , attendance_location_department_type
    -- arrival
    , attendance_arrival_date 
    , attendance_arrival_time
    , attendance_arrival_arrival_mode_code
    -- discharge
    , attendance_departure_date 
    , attendance_departure_time
    , attendance_departure_time_since_arrival 
    -- reasons for attendance
    , clinical_chief_complaint_code 
    , clinical_chief_complaint_is_injury_related
    , clinical_acuity_code 
    , attendance_location_department_type 
    -- cost
    , commissioning_national_pricing_final_price 
    -- patient demographics at time for 2ndry care only analysis
    , patient_age_at_arrival
    , patient_ethnic_category
    , patient_usual_address_lsoa_11
    , patient_usual_address_local_authority_district
 
from {{ ref('raw_sus_ae_emergency_care') }}