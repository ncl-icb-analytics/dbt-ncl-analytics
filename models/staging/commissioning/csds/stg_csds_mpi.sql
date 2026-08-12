{{
    config(materialized = 'table')
}}

with deduplicated as (
    {{
        deduplicate_csds(
            csds_table = ref('raw_csds_cyp001mpi'),
            partition_cols = ['person_id']
        )
    }}
)

select
    person_id
    , lower_super_output_area_residence as lsoa21_residence
    , lower_super_output_area_residence_2011 as lsoa11_residence
    , local_authority_district_unitary_authority as local_authority_district
    , electoral_ward_of_usual_address
    , organisation_identifier_sub_icb_location_of_residence as sub_icb_of_residence
    , person_stated_gender_code
    , person_death_date
    , dmic_ethnic_category_group
    , organisation_code_provider as org_id
from deduplicated
where person_id is not null
