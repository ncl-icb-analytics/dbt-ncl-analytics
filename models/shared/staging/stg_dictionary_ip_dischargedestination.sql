-- Staging model for dictionary_ip.DischargeDestination
-- Source: "Dictionary"."IP"
-- Description: Reference data for inpatient procedures and treatments

select
    "SK_DischargeDestinationID" as sk_discharge_destination_id,
    "BK_DischargeDestinationCode" as bk_discharge_destination_code,
    "DischargeDestinationName" as discharge_destination_name
from {{ source('dictionary_ip', 'DischargeDestination') }}
