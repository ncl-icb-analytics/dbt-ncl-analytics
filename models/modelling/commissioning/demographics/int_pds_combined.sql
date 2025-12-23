with pds_person as (
    select
        sk_patient_id,
        event_to_date is null as current_record_person,
        year_month_of_birth,
        gender_code,
        date_of_death,
        death_status,
        preferred_language_code,
        interpreter_required_code
    from {{ref('stg_pds_pds_person')}}
    
    qualify coalesce(event_to_date, '9999-12-31') = max(coalesce(event_to_date, '9999-12-31')) over (partition by sk_patient_id)
),

pds_address as (
    select 
        sk_patient_id,
        event_to_date is null as current_record_resident,
        postcode_sector,
        lsoa_21
        
    from {{ref('stg_pds_pds_address')}}

    qualify coalesce(event_to_date, '9999-12-31') = max(coalesce(event_to_date, '9999-12-31')) over (partition by sk_patient_id)
),

pds_registered as (
    select 
        sk_patient_id,
        event_to_date is null as current_record_registered,
        practice_code
        
    from {{ref('stg_pds_pds_patient_care_practice')}}

    where reason_for_removal is null

    qualify coalesce(event_to_date, '9999-12-31') = max(coalesce(event_to_date, '9999-12-31')) over (partition by sk_patient_id)
)


select 
    pds_person.*,
    pds_registered.* exclude (sk_patient_id), 
    pds_address.* exclude (sk_patient_id)
from pds_person

left join pds_registered on
pds_person.sk_patient_id = pds_registered.sk_patient_id

left join pds_address on
pds_person.sk_patient_id = pds_address.sk_patient_id