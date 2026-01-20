{{
    config(
        description="Raw layer (Personal Demographics Service data). 1:1 passthrough with cleaned column names. \nSource: DATA_LAKE.PDS.PDS_Patient_Care_Practice \ndbt: source(''pds'', ''PDS_Patient_Care_Practice'') \nColumns:\n  RowID -> row_id\n  Pseudo NHS Number -> pseudo_nhs_number\n  Primary Care Provider -> primary_care_provider\n  Primary Care Provider Business Effective From Date -> primary_care_provider_business_effective_from_date\n  Primary Care Provider Business Effective To Date -> primary_care_provider_business_effective_to_date\n  Reason for Removal -> reason_for_removal\n  derCcgOfRegistration -> der_ccg_of_registration\n  derCurrentCcgOfRegistration -> der_current_ccg_of_registration\n  derIcbOfRegistration -> der_icb_of_registration\n  derCurrentIcbOfRegistration -> der_current_icb_of_registration"
    )
}}
select
    "RowID" as row_id,
    "Pseudo NHS Number" as pseudo_nhs_number,
    "Primary Care Provider" as primary_care_provider,
    "Primary Care Provider Business Effective From Date" as primary_care_provider_business_effective_from_date,
    "Primary Care Provider Business Effective To Date" as primary_care_provider_business_effective_to_date,
    "Reason for Removal" as reason_for_removal,
    "derCcgOfRegistration" as der_ccg_of_registration,
    "derCurrentCcgOfRegistration" as der_current_ccg_of_registration,
    "derIcbOfRegistration" as der_icb_of_registration,
    "derCurrentIcbOfRegistration" as der_current_icb_of_registration
from {{ source('pds', 'PDS_Patient_Care_Practice') }}
