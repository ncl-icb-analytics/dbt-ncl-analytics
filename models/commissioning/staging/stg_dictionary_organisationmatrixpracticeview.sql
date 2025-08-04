-- Staging model for dictionary.OrganisationMatrixPracticeView
-- Source: "Dictionary"."dbo"
{% if source.get('description') %}
-- Description: Reference data including PDS and lookup tables
{% endif %}

select
    "SK_OrganisationID_Practice" as sk_organisationid_practice,
    "PracticeCode" as practicecode,
    "PracticeName" as practicename,
    "SK_OrganisationID_Network" as sk_organisationid_network,
    "NetworkCode" as networkcode,
    "NetworkName" as networkname,
    "SK_OrganisationID_Commissioner" as sk_organisationid_commissioner,
    "CommissionerCode" as commissionercode,
    "CommissionerName" as commissionername,
    "SK_OrganisationID_STP" as sk_organisationid_stp,
    "STPCode" as stpcode,
    "STPName" as stpname
from {{ source('dictionary', 'OrganisationMatrixPracticeView') }}
