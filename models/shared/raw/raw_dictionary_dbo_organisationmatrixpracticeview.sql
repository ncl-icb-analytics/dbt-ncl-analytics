-- Raw layer model for dictionary_dbo.OrganisationMatrixPracticeView
-- Source: "Dictionary"."dbo"
-- Description: Reference data including PDS and lookup tables
-- This is a 1:1 passthrough from source with standardized column names
select
    "SK_OrganisationID_Practice" as sk_organisation_id_practice,
    "PracticeCode" as practice_code,
    "PracticeName" as practice_name,
    "SK_OrganisationID_Network" as sk_organisation_id_network,
    "NetworkCode" as network_code,
    "NetworkName" as network_name,
    "SK_OrganisationID_Commissioner" as sk_organisation_id_commissioner,
    "CommissionerCode" as commissioner_code,
    "CommissionerName" as commissioner_name,
    "SK_OrganisationID_STP" as sk_organisation_id_stp,
    "STPCode" as stp_code,
    "STPName" as stp_name
from {{ source('dictionary_dbo', 'OrganisationMatrixPracticeView') }}
