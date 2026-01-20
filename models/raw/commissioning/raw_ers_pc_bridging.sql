{{
    config(
        description="Raw layer (Primary care referrals data). 1:1 passthrough with cleaned column names. \nSource: DATA_LAKE.ERS.bridging \ndbt: source(''eRS_primary_care'', ''bridging'') \nColumns:\n  Person_ID -> person_id\n  NHSNumber Pseudo -> nhs_number_pseudo"
    )
}}
select
    "Person_ID" as person_id,
    "NHSNumber Pseudo" as nhs_number_pseudo
from {{ source('eRS_primary_care', 'bridging') }}
