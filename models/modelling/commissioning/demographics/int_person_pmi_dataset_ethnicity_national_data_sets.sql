select
    sk_patientid as eth_sk_patient_id,
    --Trim to fix records with trailing spaces
    trim(ethnicity_code) as eth_ethnicity_code,
    cast(record_date as date) as eth_ethnicity_event_date
    
from {{ ref('stg_reference_lookup_ncl_ethnicity_national_data_sets') }}
