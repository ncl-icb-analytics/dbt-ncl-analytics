-- Staging model for dictionary_ip.SourceOfAdmissions
-- Source: "Dictionary"."IP"
-- Description: Reference data for inpatient procedures and treatments

select
    "SK_SourceOfAdmissionID" as sk_sourceofadmissionid,
    "BK_SourceOfAdmissionCode" as bk_sourceofadmissioncode,
    "SourceOfAdmissionName" as sourceofadmissionname,
    "SourceOfAdmissionFullName" as sourceofadmissionfullname,
    "DateCreated" as datecreated,
    "DateUpdated" as dateupdated
from {{ source('dictionary_ip', 'SourceOfAdmissions') }}
