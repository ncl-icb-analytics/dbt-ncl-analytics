-- Staging model for eRS_primary_care.bridging
-- Source: "DATA_LAKE"."ERS"
-- Description: Primary care referrals data

select
    "Person_ID" as person_id,
    "NHSNumber Pseudo" as nhsnumber_pseudo
from {{ source('eRS_primary_care', 'bridging') }}
