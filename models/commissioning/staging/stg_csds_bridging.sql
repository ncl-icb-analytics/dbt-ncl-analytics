-- Staging model for csds.Bridging
-- Source: "DATA_LAKE"."CSDS"
-- Description: Community services dataset

select
    "Person_ID" as person_id,
    "NHSNumber Pseudo" as nhs_number_pseudo,
    "Person_Index_id" as person_index_id,
    "Pseudo_NHS_Number" as pseudo_nhs_number
from {{ source('csds', 'Bridging') }}
