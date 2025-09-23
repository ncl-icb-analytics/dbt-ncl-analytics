-- Staging model for reference_analyst_managed.IMD2019
-- Source: "DATA_LAKE__NCL"."ANALYST_MANAGED"
-- Description: Analyst-managed reference datasets and business rules

select
    "LSOACODE" as lsoacode,
    "IMDDECILE" as imddecile
from {{ source('reference_analyst_managed', 'IMD2019') }}
