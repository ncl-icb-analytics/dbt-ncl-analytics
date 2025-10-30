-- Raw layer model for reference_analyst_managed.CSDS_LOOKUP
-- Source: "DATA_LAKE__NCL"."ANALYST_MANAGED"
-- Description: Analyst-managed reference datasets and business rules
-- This is a 1:1 passthrough from source with standardized column names
select
    "CSDS_FIELDNAME" as csds_fieldname,
    "SIMPLETABLE_FIELDNAME" as simpletable_fieldname,
    "CODE" as code,
    "DESCRIPTION" as description
from {{ source('reference_analyst_managed', 'CSDS_LOOKUP') }}
