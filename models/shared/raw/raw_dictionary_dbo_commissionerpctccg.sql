-- Raw layer model for dictionary_dbo.CommissionerPCTCCG
-- Source: "Dictionary"."dbo"
-- Description: Reference data including PDS and lookup tables
-- This is a 1:1 passthrough from source with standardized column names
select
    "SK_CommissionerID_PCT" as sk_commissioner_id_pct,
    "SK_CommissionerID_CCG" as sk_commissioner_id_ccg,
    "SK_PODTeamID" as sk_pod_team_id,
    "PODName" as pod_name,
    "Region" as region,
    "StartDate" as start_date,
    "EndDate" as end_date
from {{ source('dictionary_dbo', 'CommissionerPCTCCG') }}
