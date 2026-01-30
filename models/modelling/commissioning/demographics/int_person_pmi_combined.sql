with ids as (
    select distinct sk_patient_id
    from (
        select pds_sk_patient_id as sk_patient_id from {{ref('int_person_pmi_dataset_pds')}}
        union
        select sus_sk_patient_id from {{ref('int_person_pmi_dataset_sus')}}
        union
        select eth_sk_patient_id from {{ref('int_person_pmi_dataset_ethnicity_national_data_sets')}}
    )
),

combined as (
    select 
        ids.sk_patient_id,
        pds.*,
        sus.*,
        eth.*
    from ids

    left join {{ref('int_person_pmi_dataset_pds')}} pds
    on ids.sk_patient_id = pds_sk_patient_id
    
    left join {{ref('int_person_pmi_dataset_sus')}} sus
    on ids.sk_patient_id = sus_sk_patient_id 

    left join {{ref('int_person_pmi_dataset_ethnicity_national_data_sets')}} eth
    on ids.sk_patient_id = eth_sk_patient_id
),

gender_field as (
    select 
        sk_patient_id,
        case 
            when pds_gender_code is not null then 'pds'
            when sus_gender_code is not null then 'sus'
            else null
        end as gender_source,
        case 
            when gender_source = 'pds' then pds_gender_code
            when gender_source = 'sus' then sus_gender_code
            else null
        end as gender_code,
        case 
            when gender_source = 'pds' then pds_gender_event_date
            when gender_source = 'sus' then sus_gender_event_date
            else null
        end as gender_event_date
        
    from combined
),

dob_field as (
    select 
        sk_patient_id,
        case 
            when pds_date_of_birth is not null then 'pds'
            when sus_date_of_birth is not null then 'sus'
            else null
        end as dob_source,
        case 
            when dob_source = 'pds' then pds_date_of_birth
            when dob_source = 'sus' then sus_date_of_birth
            else null
        end as date_of_birth,
        case 
            when dob_source = 'pds' then pds_dob_event_date
            when dob_source = 'sus' then sus_dob_event_date
            else null
        end as dob_event_date
        
    from combined
),

date_of_death_field as (
    select 
        sk_patient_id,
        case 
            when pds_date_of_death is not null then 'pds'
            else null
        end as death_source,
        pds_date_of_death as date_of_death,
        pds_death_event_date as death_event_date
        
    from combined
),

ethnicity_field as (
    select 
        sk_patient_id,
        case 
            when eth_ethnicity_code is not null then 'ethnicity national data sets'
            when sus_ethnicity_code is not null then 'sus'
            else null
        end as ethnicity_source,
        case 
            when ethnicity_source = 'ethnicity national data sets' then eth_ethnicity_code
            when ethnicity_source = 'sus' then sus_ethnicity_code
            else null
        end as ethnicity_code,
        case 
            when ethnicity_source = 'ethnicity national data sets' then eth_ethnicity_event_date
            when ethnicity_source = 'sus' then sus_ethnicity_event_date
            else null
        end as ethnicity_event_date
        
    from combined
),

preferred_language_field as (
    select 
        sk_patient_id,
        case 
            when pds_preferred_language_code is not null then 'pds'
            else null
        end as preferred_language_source,
        pds_preferred_language_code as preferred_language_code,
        pds_preferred_language_event_date as preferred_language_event_date
        
    from combined
),

interpreter_required_field as (
    select 
        sk_patient_id,
        case 
            when pds_interpreter_required is not null then 'pds'
            else null
        end as interpreter_required_source,
        pds_interpreter_required as interpreter_required,
        pds_interpreter_event_date as interpreter_event_date
        
    from combined
),

lsoa_field as (
    select 
        sk_patient_id,
        case 
            when pds_lsoa_21 is not null then 'pds'
            when sus_lsoa_21 is not null then 'sus'
            else null
        end as lsoa_source,
        case 
            when lsoa_source = 'pds' then pds_lsoa_21
            when lsoa_source = 'sus' then sus_lsoa_21
            else null
        end as lsoa_21,
        case 
            when lsoa_source = 'pds' then pds_residence_event_date
            when lsoa_source = 'sus' then sus_residence_event_date
            else null
        end as residence_event_date
        
    from combined
),

practice_code_field as (
    select 
        sk_patient_id,
        case 
            when pds_practice_code is not null then 'pds'
            when sus_practice_code is not null then 'sus'
            else null
        end as practice_source,
        case 
            when practice_source = 'pds' then pds_practice_code
            when practice_source = 'sus' then sus_practice_code
            else null
        end as practice_code,
        case 
            when practice_source = 'pds' then pds_registered_event_date
            when practice_source = 'sus' then sus_registered_event_date
            else null
        end as registered_event_date
        
    from combined
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
    lsoa_field.lsoa_21,
    lsoa_field.residence_event_date,
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