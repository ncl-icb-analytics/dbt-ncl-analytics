{{
    config(
        description="Raw layer (Reference data including PDS and lookup tables). 1:1 passthrough with cleaned column names. \nSource: Dictionary.dbo.STPCommissioner \ndbt: source(''dictionary_dbo'', ''STPCommissioner'') \nColumns:\n  SK_STPID -> sk_stpid\n  SK_CommissionerID -> sk_commissioner_id\n  SK_OrganisationID_Commissioner -> sk_organisation_id_commissioner\n  DateCreated -> date_created\n  DateUpdated -> date_updated\n  SK_OrganisationID_STP -> sk_organisation_id_stp"
    )
}}
select
    "SK_STPID" as sk_stpid,
    "SK_CommissionerID" as sk_commissioner_id,
    "SK_OrganisationID_Commissioner" as sk_organisation_id_commissioner,
    "DateCreated" as date_created,
    "DateUpdated" as date_updated,
    "SK_OrganisationID_STP" as sk_organisation_id_stp
from {{ source('dictionary_dbo', 'STPCommissioner') }}
