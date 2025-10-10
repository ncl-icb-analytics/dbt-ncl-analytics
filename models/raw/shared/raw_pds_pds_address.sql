-- Raw layer model for pds.PDS_Address
-- Source: "DATA_LAKE"."PDS"
-- Description: Personal Demographics Service data
-- This is a 1:1 passthrough from source with standardized column names
select
    "RowID" as row_id,
    "Pseudo NHS Number" as pseudo_nhs_number,
    "derPostcodeSector" as der_postcode_sector,
    "Usual Address Business Effective From Date" as usual_address_business_effective_from_date,
    "Usual Address Business Effective To Date" as usual_address_business_effective_to_date,
    "derCqcCareHomeCode" as der_cqc_care_home_code,
    "derCcgOfResidence" as der_ccg_of_residence,
    "derLocalAuthorityOfResidence" as der_local_authority_of_residence,
    "derCurrentCcgOfResidence" as der_current_ccg_of_residence,
    "derCurrentLaOfResidence" as der_current_la_of_residence,
    "derCurrentyr2011LSOAofResidence_FromPostcode" as der_currentyr2011_lsoa_of_residence_from_postcode,
    "derCurrentyr2021LSOAofResidence_FromPostcode" as der_currentyr2021_lsoa_of_residence_from_postcode,
    "dmicPostcodeTypeId" as dmic_postcode_type_id,
    "dmicPostcodeWellbeingTypeId" as dmic_postcode_wellbeing_type_id,
    "derIcbOfResidence" as der_icb_of_residence,
    "derCurrentIcbOfResidence" as der_current_icb_of_residence,
    "derCurrentyr2021ElectoralWardOfResidence_FromPostcode" as der_currentyr2021_electoral_ward_of_residence_from_postcode
from {{ source('pds', 'PDS_Address') }}
