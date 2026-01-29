--When expanding this change int tables to use the same names and then union by name
with combined as (
    select * from {{ref('int_person_pmi_dataset_pds')}}
    union by name
    select * from {{ref('int_person_pmi_dataset_sus')}}
    union by name
    select * from {{ref('int_person_pmi_dataset_ethnicity_national_data_sets')}}
),

ids as (
    select distinct sk_patient_id
    from combined
),

gender_field as (
    select
        sk_patient_id,
        dataset_source as gender_source,
        gender_code,
        gender_event_date
        
    from combined

    where dataset_source in ('pds', 'sus')
    
    qualify row_number () over (
        partition by sk_patient_id
        order by 
            gender_code in ('1','2','9') desc,
            gender_event_date desc,
            dataset_source = 'pds' desc
    ) = 1
),

dob_field as (
    select 
        sk_patient_id,
        dataset_source as dob_source,
        date_of_birth,
        dob_event_date
        
    from combined

    where dataset_source in ('pds', 'sus')

    qualify row_number () over (
        partition by sk_patient_id
        order by 
            date_of_birth is not null desc,
            dataset_source = 'pds' desc,
            dob_event_date desc
    ) = 1
),

date_of_death_field as (
    select
        sk_patient_id,
        dataset_source as death_source,
        date_of_death,
        death_event_date
        
    from combined
    where dataset_source = 'pds'
),

ethnicity_field as (
    select 

        sk_patient_id,
        dataset_source as ethnicity_source,
        ethnicity_code,
        ethnicity_event_date
        
    from combined

    where dataset_source in ('eth', 'sus')

    qualify row_number () over (
        partition by sk_patient_id
        order by 
            ethnicity_code is not null desc,
            ethnicity_code not in ('X','Z','99') desc,
            dataset_source = 'eth' desc,
            ethnicity_event_date desc
    ) = 1
),

preferred_language_field as (
    select
        sk_patient_id,
        dataset_source as preferred_language_source,
        preferred_language_code,
        preferred_language_event_date
        
    from combined
    where dataset_source = 'pds'
),

interpreter_required_field as (

    select
        sk_patient_id,
        dataset_source as interpreter_required_source,
        interpreter_required,
        interpreter_event_date
        
    from combined
    where dataset_source = 'pds'
),

lsoa_field as (
    select 

        sk_patient_id,
        dataset_source as lsoa_source,
        lsoa21_code,
        lsoa_event_date
        
    from combined

    where dataset_source in ('pds', 'sus')

    qualify row_number () over (
        partition by sk_patient_id
        order by 
            lsoa21_code is not null desc,
            lsoa_event_date desc,
            dataset_source = 'pds' desc
    ) = 1
),
practice_code_field as (
    select 

        sk_patient_id,
        dataset_source as practice_source,
        practice_code,
        registered_event_date
        
    from combined

    where dataset_source in ('pds', 'sus')

    qualify row_number () over (
        partition by sk_patient_id
        order by 
            practice_code is not null desc,
            registered_event_date desc,
            dataset_source = 'pds' desc
    ) = 1
)

select
    ids.sk_patient_id,
    gender_field.gender_source,
    gender_field.gender_code,
    gender_field.gender_event_date,
    dob_field.dob_source,
    dob_field.date_of_birth,
    dob_field.dob_event_date,
    date_of_death_field.death_source,
    date_of_death_field.date_of_death,
    date_of_death_field.death_event_date,
    ethnicity_field.ethnicity_source,
    ethnicity_field.ethnicity_code,
    ethnicity_field.ethnicity_event_date,
    preferred_language_field.preferred_language_source,
    preferred_language_field.preferred_language_code,
    preferred_language_field.preferred_language_event_date,
    interpreter_required_field.interpreter_required_source,
    interpreter_required_field.interpreter_required,
    interpreter_required_field.interpreter_event_date,
    lsoa_field.lsoa_source,
    lsoa_field.lsoa21_code,
    lsoa_field.lsoa_event_date,
    practice_code_field.practice_source,
    practice_code_field.practice_code,
    practice_code_field.registered_event_date,
    ncl_flags.flag_current_ncl_registered,
    ncl_flags.record_registered_start_date,
    ncl_flags.flag_current_ncl_residence,
    ncl_flags.record_residence_start_date

from ids

left join gender_field
on ids.sk_patient_id = gender_field.sk_patient_id

left join dob_field
on ids.sk_patient_id = dob_field.sk_patient_id

left join date_of_death_field
on ids.sk_patient_id = date_of_death_field.sk_patient_id

left join ethnicity_field
on ids.sk_patient_id = ethnicity_field.sk_patient_id

left join preferred_language_field
on ids.sk_patient_id = preferred_language_field.sk_patient_id

left join interpreter_required_field
on ids.sk_patient_id = interpreter_required_field.sk_patient_id

left join lsoa_field
on ids.sk_patient_id = lsoa_field.sk_patient_id

left join practice_code_field
on ids.sk_patient_id = practice_code_field.sk_patient_id

left join {{ref('int_person_pds_ncl_population_flags')}} ncl_flags
on ids.sk_patient_id = ncl_flags.sk_patient_id