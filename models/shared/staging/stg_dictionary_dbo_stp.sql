-- Staging model for dictionary_dbo.STP
-- Source: "Dictionary"."dbo"
-- Description: Reference data including PDS and lookup tables

select
    "SK_STPID" as sk_stpid,
    "STPCode" as stpcode,
    "STPName" as stpname,
    "DateCreated" as datecreated,
    "DateUpdated" as dateupdated,
    "SK_ServiceProviderGroupID" as sk_serviceprovidergroupid,
    "ODSCode" as odscode,
    "SK_OrganisationID" as sk_organisationid
from {{ source('dictionary_dbo', 'STP') }}
