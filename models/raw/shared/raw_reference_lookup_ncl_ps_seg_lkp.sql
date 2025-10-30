-- Raw layer model for reference_lookup_ncl.PS_SEG_LKP
-- Source: "MODELLING"."LOOKUP_NCL"
-- Description: Analyst-managed reference datasets and business rules in the MODELLING environment
-- This is a 1:1 passthrough from source with standardized column names
select
    "POPSEG" as popseg,
    "POPSEG_DESC" as popseg_desc
from {{ source('reference_lookup_ncl', 'PS_SEG_LKP') }}
