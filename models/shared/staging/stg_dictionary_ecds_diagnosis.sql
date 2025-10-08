select
    ecds_unique_id,
    refset_unique_id,
    snomed_code,
    snomed_uk_preferred_term,
    snomed_fully_specified_name,
    ecds_description,
    ecds_group1,
    ecds_group2,
    ecds_group3,
    flag_allergy,
    flag_notifiable_disease,
    flag_injury,
    flag_male,
    flag_female,
    flag_sdec,
    flag_ads,
    icd10_mapping,
    icd10_description,
    icd11_mapping,
    icd11_description,
    valid_from,
    valid_to,
    dv_is_active
    -- Excluded (low analytical value):
    -- sort1, sort2, sort3, sort4, notes
from {{ ref('raw_dictionary_ecds_diagnosis') }}