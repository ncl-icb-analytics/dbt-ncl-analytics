-- Staging model for dictionary_ip.DischargeMethod
-- Source: "Dictionary"."IP"
-- Description: Reference data for inpatient procedures and treatments

select
    "SK_DischargeMethodID" as sk_dischargemethodid,
    "BK_DischargeMethodCode" as bk_dischargemethodcode,
    "DischargeMethodName" as dischargemethodname
from {{ source('dictionary_ip', 'DischargeMethod') }}
