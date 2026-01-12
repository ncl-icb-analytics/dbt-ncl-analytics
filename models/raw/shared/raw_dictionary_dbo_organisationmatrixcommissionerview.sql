{{
    config(
        description="Raw layer (Reference data including PDS and lookup tables). 1:1 passthrough with cleaned column names. \nSource: Dictionary.dbo.OrganisationMatrixCommissionerView \ndbt: source(''dictionary_dbo'', ''OrganisationMatrixCommissionerView'') \nColumns:\n  SK_OrganisationID_Commissioner -> sk_organisation_id_commissioner\n  CommissionerCode -> commissioner_code\n  CommissionerName -> commissioner_name\n  SK_OrganisationID_STP -> sk_organisation_id_stp\n  STPCode -> stp_code\n  STPName -> stp_name"
    )
}}
select
    "SK_OrganisationID_Commissioner" as sk_organisation_id_commissioner,
    "CommissionerCode" as commissioner_code,
    "CommissionerName" as commissioner_name,
    "SK_OrganisationID_STP" as sk_organisation_id_stp,
    "STPCode" as stp_code,
    "STPName" as stp_name
from {{ source('dictionary_dbo', 'OrganisationMatrixCommissionerView') }}
