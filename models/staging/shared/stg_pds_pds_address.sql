select
    row_id,
    pseudo_nhs_number as sk_patient_id, 
    der_postcode_sector as postcode_sector, 
    der_currentyr2021_lsoa_of_residence_from_postcode as lsoa_21, 
    usual_address_business_effective_from_date as event_from_date, 
    usual_address_business_effective_to_date as event_to_date

from {{ref('raw_pds_pds_address')}}