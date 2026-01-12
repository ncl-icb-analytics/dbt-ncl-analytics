{{
    config(
        description="Raw layer (Personal Demographics Service data). 1:1 passthrough with cleaned column names. \nSource: DATA_LAKE.PDS.PDS_Address \ndbt: source(''pds'', ''PDS_Address'') \nColumns:\n  RowID -> row_id\n  Pseudo NHS Number -> pseudo_nhs_number\n  derPostcodeSector -> der_postcode_sector\n  Usual Address Business Effective From Date -> usual_address_business_effective_from_date\n  Usual Address Business Effective To Date -> usual_address_business_effective_to_date\n  derCqcCareHomeCode -> der_cqc_care_home_code\n  derCcgOfResidence -> der_ccg_of_residence\n  derLocalAuthorityOfResidence -> der_local_authority_of_residence\n  derCurrentCcgOfResidence -> der_current_ccg_of_residence\n  derCurrentLaOfResidence -> der_current_la_of_residence\n  derCurrentyr2011LSOAofResidence_FromPostcode -> der_currentyr2011_lsoa_of_residence_from_postcode\n  derCurrentyr2021LSOAofResidence_FromPostcode -> der_currentyr2021_lsoa_of_residence_from_postcode\n  dmicPostcodeTypeId -> dmic_postcode_type_id\n  dmicPostcodeWellbeingTypeId -> dmic_postcode_wellbeing_type_id\n  derIcbOfResidence -> der_icb_of_residence\n  derCurrentIcbOfResidence -> der_current_icb_of_residence\n  derCurrentyr2021ElectoralWardOfResidence_FromPostcode -> der_currentyr2021_electoral_ward_of_residence_from_postcode"
    )
}}
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
