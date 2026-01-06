--CTEs to combine the 3 base PDS tables, only including the latest record for each patient
with pds_person as (
    select
        sk_patient_id,
        event_from_date as record_person_start_date,
        event_to_date as record_person_end_date,
        year_month_of_birth,
        gender_code,
        date_of_death,
        preferred_language_code,
        interpreter_required
    from {{ref('stg_pds_pds_person')}}
    
    --Get the latest record
    qualify row_number() over (
        partition by sk_patient_id
        order by 
            coalesce(event_to_date, '9999-12-31') desc,
            event_from_date desc,
            row_id desc
    ) = 1
),

pds_address as (
    select 
        sk_patient_id,
        event_from_date as record_residence_start_date,
        event_to_date as record_residence_end_date,
        postcode_sector,
        lsoa_21
        
    from {{ref('stg_pds_pds_address')}}

    --Get the latest record
    qualify row_number() over (
        partition by sk_patient_id
        order by 
            coalesce(event_to_date, '9999-12-31') desc,
            event_from_date desc,
            row_id desc
    ) = 1
),

pds_registered as (
    select 
        sk_patient_id,
        event_from_date as record_registered_start_date,
        event_to_date as record_registered_end_date,
        practice_code,
        registered_reason_for_removal
        
    from {{ref('stg_pds_pds_patient_care_practice')}}

    --Get the latest record
    qualify row_number() over (
        partition by sk_patient_id
        order by 
            coalesce(event_to_date, '9999-12-31') desc,
            event_from_date desc,
            row_id desc
    ) = 1
)

--Script to combine the 3 PDS data tables into a single wider table
select 
    pds_person.*,
    pds_registered.* exclude (sk_patient_id), 
    pds_address.* exclude (sk_patient_id)
from pds_person

left join pds_registered on
pds_person.sk_patient_id = pds_registered.sk_patient_id

left join pds_address on
pds_person.sk_patient_id = pds_address.sk_patient_id