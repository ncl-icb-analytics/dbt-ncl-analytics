{{
    config(materialized = 'table')
}}

with deduplicated as (
    {{
        deduplicate_csds(
            csds_table = ref('raw_csds_cyp002gp'),
            partition_cols = [
                'person_id',
                'general_medical_practice_code_patient_registration',
                'start_date_gmp_patient_registration'
            ]
        )
    }}
)

select
    person_id
    -- ~19% of source rows carry lowercase codes; uppercase before use and
    -- collapse case variants of the same registration below
    , upper(general_medical_practice_code_patient_registration) as practice_code
    , start_date_gmp_patient_registration as registration_start_date
    , end_date_gmp_patient_registration as registration_end_date
    , upper(organisation_code_ccg_of_gp_practice) as organisation_code_ccg_of_gp_practice
from deduplicated
where person_id is not null
    and general_medical_practice_code_patient_registration is not null
qualify row_number() over (
    partition by person_id
        , upper(general_medical_practice_code_patient_registration)
        , start_date_gmp_patient_registration
    order by effective_from desc
) = 1
