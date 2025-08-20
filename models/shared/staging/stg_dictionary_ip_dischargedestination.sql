-- Staging model for dictionary_ip.DischargeDestination
-- Source: "Dictionary"."IP"
-- Description: Reference data for inpatient procedures and treatments

select
    "SK_DischargeDestinationID" as sk_dischargedestinationid,
    "BK_DischargeDestinationCode" as bk_dischargedestinationcode,
    "DischargeDestinationName" as dischargedestinationname
from {{ source('dictionary_ip', 'DischargeDestination') }}
