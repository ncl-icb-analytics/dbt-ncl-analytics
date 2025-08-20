-- Staging model for dictionary_dbo.CommissionerMatrixNational
-- Source: "Dictionary"."dbo"
-- Description: Reference data including PDS and lookup tables

select
    "SK_CommissionerID" as sk_commissionerid,
    "SK_CommissionerGroupID" as sk_commissionergroupid,
    "SK_CommissionerOrgID" as sk_commissionerorgid,
    "SK_PODTeamID" as sk_podteamid,
    "SK_PODTeamGroupID" as sk_podteamgroupid,
    "SK_STPID" as sk_stpid,
    "SK_STPGroupID" as sk_stpgroupid,
    "SK_PCTID" as sk_pctid,
    "DateCreated" as datecreated,
    "DateUpdated" as dateupdated
from {{ source('dictionary_dbo', 'CommissionerMatrixNational') }}
