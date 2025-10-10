-- Raw layer model for eRS_primary_care.bridging
-- Source: "DATA_LAKE"."ERS"
-- Description: Primary care referrals data
-- This is a 1:1 passthrough from source with standardized column names
select
    "Person_ID" as person_id,
    "NHSNumber Pseudo" as nhs_number_pseudo
from {{ source('eRS_primary_care', 'bridging') }}
