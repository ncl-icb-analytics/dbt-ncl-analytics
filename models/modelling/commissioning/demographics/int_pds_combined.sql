--CTEs to combine the 3 base PDS tables, only including the latest record for each patient
with pds_person as (
    select
        sk_patient_id,
        (event_to_date is null and date_of_death is null) as current_record_person,
        event_to_date as record_end_date,
        year_month_of_birth,
        gender_code,
        date_of_death,
        preferred_language_code,
        interpreter_required
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