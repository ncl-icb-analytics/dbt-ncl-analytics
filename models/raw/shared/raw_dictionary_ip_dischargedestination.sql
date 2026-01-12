{{
    config(
        description="Raw layer (Reference data for inpatient procedures and treatments). 1:1 passthrough with cleaned column names. \nSource: Dictionary.IP.DischargeDestination \ndbt: source(''dictionary_ip'', ''DischargeDestination'') \nColumns:\n  SK_DischargeDestinationID -> sk_discharge_destination_id\n  BK_DischargeDestinationCode -> bk_discharge_destination_code\n  DischargeDestinationName -> discharge_destination_name"
    )
}}
select
    "SK_DischargeDestinationID" as sk_discharge_destination_id,
    "BK_DischargeDestinationCode" as bk_discharge_destination_code,
    "DischargeDestinationName" as discharge_destination_name
from {{ source('dictionary_ip', 'DischargeDestination') }}
