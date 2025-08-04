-- Staging model for dictionary.Consultant
-- Source: "Dictionary"."dbo"
{% if source.get('description') %}
-- Description: Reference data including PDS and lookup tables
{% endif %}

select
    "SK_ConsultantID" as sk_consultantid,
    "GMCCode" as gmccode,
    "Surname" as surname,
    "Initials" as initials,
    "GMCName" as gmcname,
    "SexCode" as sexcode,
    "Specialty_Function_Code" as specialty_function_code,
    "Location_Organisation_Code" as location_organisation_code,
    "DateCreated" as datecreated,
    "DateUpdated" as dateupdated
from {{ source('dictionary', 'Consultant') }}
