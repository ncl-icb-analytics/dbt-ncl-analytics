{{
    config(
        description="Raw layer (Reference data for inpatient procedures and treatments). 1:1 passthrough with cleaned column names. \nSource: Dictionary.IP.DischargeMethod \ndbt: source(''dictionary_ip'', ''DischargeMethod'') \nColumns:\n  SK_DischargeMethodID -> sk_discharge_method_id\n  BK_DischargeMethodCode -> bk_discharge_method_code\n  DischargeMethodName -> discharge_method_name"
    )
}}
select
    "SK_DischargeMethodID" as sk_discharge_method_id,
    "BK_DischargeMethodCode" as bk_discharge_method_code,
    "DischargeMethodName" as discharge_method_name
from {{ source('dictionary_ip', 'DischargeMethod') }}
