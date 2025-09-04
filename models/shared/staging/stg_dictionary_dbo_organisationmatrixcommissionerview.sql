-- Staging model for dictionary_dbo.OrganisationMatrixCommissionerView
-- Source: "Dictionary"."dbo"
-- Description: Reference data including PDS and lookup tables

select
    "SK_OrganisationID_Commissioner" as sk_organisation_id_commissioner,
    "CommissionerCode" as commissioner_code,
    "CommissionerName" as commissioner_name,
    "SK_OrganisationID_STP" as sk_organisation_id_stp,
    "STPCode" as stp_code,
    "STPName" as stp_name
from {{ source('dictionary_dbo', 'OrganisationMatrixCommissionerView') }}
