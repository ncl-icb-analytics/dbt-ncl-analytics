-- Raw layer model for dictionary_dbo.OverseasVisitorStatusClassification
-- Source: "Dictionary"."dbo"
-- Description: Reference data including PDS and lookup tables
-- This is a 1:1 passthrough from source with standardized column names
select
    "SK_OverseasVisitorStatusClassificationID" as sk_overseas_visitor_status_classification_id,
    "BK_OverseasVisitorStatusClassification" as bk_overseas_visitor_status_classification,
    "OverseasVisitorStatusClassification" as overseas_visitor_status_classification
from {{ source('dictionary_dbo', 'OverseasVisitorStatusClassification') }}
