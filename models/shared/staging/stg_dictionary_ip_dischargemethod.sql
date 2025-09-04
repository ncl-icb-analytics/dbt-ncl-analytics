-- Staging model for dictionary_ip.DischargeMethod
-- Source: "Dictionary"."IP"
-- Description: Reference data for inpatient procedures and treatments

select
    "SK_DischargeMethodID" as sk_discharge_method_id,
    "BK_DischargeMethodCode" as bk_discharge_method_code,
    "DischargeMethodName" as discharge_method_name
from {{ source('dictionary_ip', 'DischargeMethod') }}
