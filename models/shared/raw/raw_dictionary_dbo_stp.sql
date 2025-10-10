-- Raw layer model for dictionary_dbo.STP
-- Source: "Dictionary"."dbo"
-- Description: Reference data including PDS and lookup tables
-- This is a 1:1 passthrough from source with standardized column names
select
    "SK_STPID" as sk_stpid,
    "STPCode" as stp_code,
    "STPName" as stp_name,
    "DateCreated" as date_created,
    "DateUpdated" as date_updated,
    "SK_ServiceProviderGroupID" as sk_service_provider_group_id,
    "ODSCode" as ods_code,
    "SK_OrganisationID" as sk_organisation_id
from {{ source('dictionary_dbo', 'STP') }}
