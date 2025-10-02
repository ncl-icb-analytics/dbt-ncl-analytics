-- Raw layer model for dictionary_dbo.F&AParentChild
-- Source: "Dictionary"."dbo"
-- Description: Reference data including PDS and lookup tables
-- This is a 1:1 passthrough from source with standardized column names
select
    "SK_PARENT_PODID" as sk_parent_podid,
    "SK_CHILD_PODID" as sk_child_podid
from {{ source('dictionary_dbo', 'F&AParentChild') }}
