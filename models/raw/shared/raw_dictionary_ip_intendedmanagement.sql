{{
    config(
        description="Raw layer (Reference data for inpatient procedures and treatments). 1:1 passthrough with cleaned column names. \nSource: Dictionary.IP.IntendedManagement \ndbt: source(''dictionary_ip'', ''IntendedManagement'') \nColumns:\n  SK_IntendedManagementID -> sk_intended_management_id\n  BK_IntendedManagementCode -> bk_intended_management_code\n  IntendedManagementDescription -> intended_management_description"
    )
}}
select
    "SK_IntendedManagementID" as sk_intended_management_id,
    "BK_IntendedManagementCode" as bk_intended_management_code,
    "IntendedManagementDescription" as intended_management_description
from {{ source('dictionary_ip', 'IntendedManagement') }}
