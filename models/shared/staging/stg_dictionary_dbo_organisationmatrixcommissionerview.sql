-- Staging model for dictionary_dbo.OrganisationMatrixCommissionerView
-- Source: "Dictionary"."dbo"
-- Description: Reference data including PDS and lookup tables

select
    "SK_OrganisationID_Commissioner" as sk_organisationid_commissioner,
    "CommissionerCode" as commissionercode,
    "CommissionerName" as commissionername,
    "SK_OrganisationID_STP" as sk_organisationid_stp,
    "STPCode" as stpcode,
    "STPName" as stpname
from {{ source('dictionary_dbo', 'OrganisationMatrixCommissionerView') }}
