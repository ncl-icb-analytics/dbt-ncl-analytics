-- Staging model for dictionary.PatientClassification
-- Source: "Dictionary"."dbo"
{% if source.get('description') %}
-- Description: Reference data including PDS and lookup tables
{% endif %}

select
    "SK_PatientClassificationID" as sk_patientclassificationid,
    "BK_PatientClassificationCode" as bk_patientclassificationcode,
    "PatientClassificationName" as patientclassificationname,
    "PatientClassificationFullName" as patientclassificationfullname
from {{ source('dictionary', 'PatientClassification') }}
