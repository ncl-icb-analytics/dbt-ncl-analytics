select  sk_patient_id
    , caci_acorn_household_type
    , caci_household_acorn_type_description
    , der_living_alone_flag
    , der_total_household_population
    , os_uprn_property_classification
    , os_uprn_property_classification_description
from {{ ref('stg_acorn_dscro_mpi') }}
where der_care_home_residence_flag = 'N'