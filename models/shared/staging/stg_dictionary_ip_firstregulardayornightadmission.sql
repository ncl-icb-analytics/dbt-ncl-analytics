-- Staging model for dictionary_ip.FirstRegularDayOrNightAdmission
-- Source: "Dictionary"."IP"
-- Description: Reference data for inpatient procedures and treatments

select
    "SK_FirstRegularDayOrNightAdmissionID" as sk_firstregulardayornightadmissionid,
    "BK_FirstRegularDayOrNightAdmission" as bk_firstregulardayornightadmission,
    "FirstRegularDayOrNightAdmission" as firstregulardayornightadmission
from {{ source('dictionary_ip', 'FirstRegularDayOrNightAdmission') }}
