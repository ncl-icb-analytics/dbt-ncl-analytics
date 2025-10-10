-- Raw layer model for dictionary_dbo.STPCommissioner
-- Source: "Dictionary"."dbo"
-- Description: Reference data including PDS and lookup tables
-- This is a 1:1 passthrough from source with standardized column names
select
    "SK_STPID" as sk_stpid,
    "SK_CommissionerID" as sk_commissioner_id,
    "SK_OrganisationID_Commissioner" as sk_organisation_id_commissioner,
    "DateCreated" as date_created,
    "DateUpdated" as date_updated,
    "SK_OrganisationID_STP" as sk_organisation_id_stp
from {{ source('dictionary_dbo', 'STPCommissioner') }}
