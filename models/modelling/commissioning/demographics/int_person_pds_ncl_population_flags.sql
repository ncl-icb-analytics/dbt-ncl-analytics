select
    sk_patient_id,
    --Flag for current NCL Registered population
    (
        --Practice is in NCL and active
        gp_lu.gp_practice_code is not null and

        --No record of death
        pds.date_of_death is null and

        --No record of the registration being removed
        pds.registered_reason_for_removal is null and

        --The GP registration has no end date (active)
        pds.record_registered_end_date is null
    ) as flag_current_ncl_registered,

    --Flag for current NCL Resident population
    (
        --LSOA 21 is in NCL
        geo.resident_flag = 'NCL' and

        --No record of death
        pds.date_of_death is null and

        --The residence record has no end date (active)
        pds.record_residence_end_date is null
    ) as flag_current_ncl_residence
    
from {{ref('int_person_pds_latest_record')}} pds

left join {{ref('stg_reference_lookup_ncl_gp_practice')}} gp_lu
on pds.practice_code = gp_lu.gp_practice_code

left join {{ref('stg_reference_lookup_ncl_lsoa_2021_ward_2025_local_authority_2025')}} geo
on pds.lsoa_21 = geo.lsoa_2021_code;
