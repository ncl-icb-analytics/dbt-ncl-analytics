select {{ consistent_sk_patient_id_format('pseudo_nhs_number') }} as sk_patient_id
    , pseudo_uprn
    , caci_acorn_household_type
    , caci_household_acorn_type_description
    , der_care_home_residence_flag
    , der_care_home_service_type
    , carehome_cqc_loc_id
    , carehome_ods_code
    , der_living_alone_flag
    , der_total_household_population
    , os_uprn_property_classification
    , os_uprn_property_classification_description
    , dmic_row_identifier
    , dmic_row_is_latest

from {{ ref('raw_acorn_dscro_mpi') }}
where pseudo_nhs_number is not null and pseudo_nhs_number <> '1'
and dmic_row_is_latest = true