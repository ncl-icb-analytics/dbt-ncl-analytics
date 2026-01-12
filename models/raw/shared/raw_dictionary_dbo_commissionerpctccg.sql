{{
    config(
        description="Raw layer (Reference data including PDS and lookup tables). 1:1 passthrough with cleaned column names. \nSource: Dictionary.dbo.CommissionerPCTCCG \ndbt: source(''dictionary_dbo'', ''CommissionerPCTCCG'') \nColumns:\n  SK_CommissionerID_PCT -> sk_commissioner_id_pct\n  SK_CommissionerID_CCG -> sk_commissioner_id_ccg\n  SK_PODTeamID -> sk_pod_team_id\n  PODName -> pod_name\n  Region -> region\n  StartDate -> start_date\n  EndDate -> end_date"
    )
}}
select
    "SK_CommissionerID_PCT" as sk_commissioner_id_pct,
    "SK_CommissionerID_CCG" as sk_commissioner_id_ccg,
    "SK_PODTeamID" as sk_pod_team_id,
    "PODName" as pod_name,
    "Region" as region,
    "StartDate" as start_date,
    "EndDate" as end_date
from {{ source('dictionary_dbo', 'CommissionerPCTCCG') }}
